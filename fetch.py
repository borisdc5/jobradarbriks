"""
JobRadar Briks — Agrégateur d'offres immobilier / construction / BTP
Scrape France Travail + APEC, enrichit via RecruitCRM, génère docs/index.html
"""
import urllib.request, urllib.parse, ssl, json, re, os, gzip, time
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor

# ── Secrets ──────────────────────────────────────────────────────────────────
FT_CLIENT_ID     = os.getenv('FT_CLIENT_ID', '')
FT_CLIENT_SECRET = os.getenv('FT_CLIENT_SECRET', '')
APEC_EMAIL       = os.getenv('APEC_EMAIL', '')
APEC_PASSWORD    = os.getenv('APEC_PASSWORD', '')
RCRM_KEY         = os.getenv('RECRUITCRM_API_KEY', '')

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
OUT       = os.path.join(BASE_DIR, 'docs', 'index.html')
TEMPLATE  = os.path.join(BASE_DIR, 'template.html')
CRM_CACHE = os.path.join(BASE_DIR, 'crm_cache.json')

# ── SSL context (permissif pour les APIs FR) ──────────────────────────────────
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode    = ssl.CERT_NONE

NOW = datetime.now(timezone.utc)

# ─────────────────────────────────────────────────────────────────────────────
# CLASSIFICATION BTP / IMMOBILIER
# ─────────────────────────────────────────────────────────────────────────────

# Mots-clés → catégorie (ordre = priorité)
CATEGORY_RULES = [
    ('Promotion Immobilière', [
        'directeur de programmes', 'responsable programmes', 'chargé de programmes',
        'chargé de programme', 'directeur programme', 'chef de projet immobilier',
        'montage d\'opération', 'développeur immobilier', 'développement immobilier',
        'promoteur', 'promotion immobilière', 'responsable opérations',
        'chargé d\'opérations', 'aménageur', 'lotissement',
    ]),
    ('Transaction / Vente', [
        'négociateur immobilier', 'agent immobilier', 'conseiller immobilier',
        'consultant immobilier', 'transaction immobilière', 'commercialisation immobilière',
        'commercial immobilier', 'vente immobilier', 'conseiller de vente immobilier',
        'agent commercial immobilier',
    ]),
    ('Asset & Property Management', [
        'asset manager', 'asset management', 'property manager', 'property management',
        'facility manager', 'facility management', 'gestionnaire de patrimoine',
        'gestionnaire d\'actifs', 'gestion locative', 'administrateur de biens',
        'gestionnaire immobilier', 'responsable patrimoine', 'exploitation immobilière',
        'gérant d\'immeuble', 'directeur patrimoine', 'directeur immobilier',
    ]),
    ('Expertise / Évaluation', [
        'expert immobilier', 'évaluateur immobilier', 'expertise immobilière',
        'évaluation immobilière', 'estimation immobilière', 'expert en évaluation',
        'certificateur', 'diagnostiqueur', 'expert technique',
    ]),
    ('Architecture & Urbanisme', [
        'architecte', 'architecte d\'intérieur', 'urbaniste', 'aménagement urbain',
        'paysagiste', 'concepteur', 'architecture', 'urbanisme',
        'aménagement du territoire', 'chef de projet architecture',
    ]),
    ('Maîtrise d\'Œuvre', [
        'maître d\'œuvre', 'maîtrise d\'œuvre', 'moe ', ' moe', 'maîtrise d\'ouvrage',
        'moa ', ' moa', 'amo ', ' amo', 'conducteur d\'opération',
        'assistant maître d\'ouvrage', 'assistant à maîtrise d\'ouvrage',
        'chargé de maîtrise d\'œuvre',
    ]),
    ('Bureau d\'Études / Ingénierie', [
        'bureau d\'études', 'ingénieur structure', 'ingénieur béton', 'ingénieur génie civil',
        'ingénieur fluides', 'ingénieur cvc', 'ingénieur thermique', 'ingénieur acoustique',
        'ingénieur vrd', 'vrd', 'géotechnique', 'topographe', 'économiste de la construction',
        'métreur', 'opc', 'chargé d\'études techniques', 'ingénieur calcul',
        'ingénieur bâtiment', 'responsable bureau d\'études',
    ]),
    ('Conduite de Travaux', [
        'conducteur de travaux', 'chef de chantier', 'directeur de travaux',
        'ingénieur travaux', 'gros œuvre', 'responsable de chantier',
        'chef de projet travaux', 'responsable travaux', 'directeur de chantier',
        'coordinateur travaux', 'technicien travaux',
    ]),
    ('Commercial / Foncier', [
        'responsable foncier', 'chargé de foncier', 'développement foncier',
        'acquisition foncière', 'foncier', 'chargé d\'affaires btp',
        'chargé d\'affaires immobilier', 'business developer immobilier',
        'responsable développement', 'prospection foncière',
    ]),
    ('Juridique & Finance Immo', [
        'juriste immobilier', 'droit immobilier', 'droit de la construction',
        'notaire', 'investissement immobilier', 'financement immobilier',
        'analyste immobilier', 'contrôleur de gestion immobilier',
        'daf immobilier', 'finance immobilier', 'scpi', 'opci',
        'responsable financier immobilier',
    ]),
    ('Support & Admin', [
        'assistant immobilier', 'assistante immobilier', 'coordinateur',
        'office manager', 'secrétaire', 'comptable immobilier',
        'ressources humaines', 'rh', 'administration',
    ]),
]

# Cabinets de recrutement spécialisés immo/BTP (à taguer comme recruteurs)
RECRUITMENT_FIRMS = {
    'hays', 'michael page', 'jones lang lasalle', 'jll', 'cbre',
    'colliers', 'cushman & wakefield', 'bnt rh', 'fed immobilier',
    'fed construction', 'adecco', 'manpower', 'randstad', 'page personnel',
    'robert half', 'talentup', 'genesis rh', 'aptitude rh',
    'talentis', 'etude et projet', 'nextep hr', 'lynx rh',
    'alphéa conseil', 'triangle intérim', 'synergie', 'proman',
    'temporis', 'adequat', 'acass', 'cabinet de recrutement',
    'consultant rh', 'chasseur de tête',
}

# Tailles d'entreprises connues dans le secteur
COMPANY_SIZES = {
    # Grands groupes
    'bouygues construction': '5k+',
    'bouygues immobilier': '5k+',
    'vinci construction': '5k+',
    'vinci immobilier': '1k-5k',
    'nexity': '5k+',
    'unibail-rodamco-westfield': '5k+',
    'gecina': '201-1k',
    'icade': '201-1k',
    'altarea': '1k-5k',
    'cogedim': '1k-5k',
    'eiffage construction': '5k+',
    'eiffage immobilier': '1k-5k',
    'kaufman & broad': '1k-5k',
    'pierre & vacances': '5k+',
    'foncière des régions': '201-1k',
    'covivio': '201-1k',
    'primonial': '1k-5k',
    'la française': '201-1k',
    'axa im real assets': '201-1k',
    'bnp paribas real estate': '5k+',
    'jll': '5k+',
    'cbre': '5k+',
    'colliers': '5k+',
    'cushman & wakefield': '5k+',
    'savills': '5k+',
    'knight frank': '1k-5k',
    'orpi': '5k+',
    'century 21': '5k+',
    'era immobilier': '5k+',
    'laforêt': '5k+',
    'guy hoquet': '1k-5k',
    'foncia': '5k+',
    'citya immobilier': '5k+',
    'square habitat': '1k-5k',
    'crédit agricole immobilier': '5k+',
    'serl': '51-200',
    'sogeprom': '51-200',
    'hermès immobilier': '11-50',
    # ESNs BTP / bureaux d'études
    'tractebel': '5k+',
    'artelia': '1k-5k',
    'setec': '1k-5k',
    'egis': '5k+',
    'systra': '1k-5k',
    'arcadis': '5k+',
    'jacobs': '5k+',
    'khephren ingénierie': '51-200',
    'tpi': '51-200',
    'gamba & associés': '11-50',
    'ingerop': '1k-5k',
    'setur': '201-1k',
    'les charpentiers de paris': '51-200',
    # Cabinets d'archi
    'rpbw': '51-200',
    'dominique perrault architecture': '51-200',
    'wilmotte & associés': '51-200',
    'agence valode et pistre': '51-200',
}


def normalize(s):
    return (s or '').lower().strip()

def classify_category(title, description=''):
    text = normalize(title + ' ' + description)
    text = re.sub(r'[./]', ' ', text)  # "Chef.Fe" / "H/F" → espaces, pour matcher les mots-clés
    for cat, keywords in CATEGORY_RULES:
        for kw in keywords:
            if kw in text:
                return cat
    return 'Autre'

def is_recruitment_firm(company):
    co = normalize(company)
    return any(firm in co for firm in RECRUITMENT_FIRMS)

def get_company_size(company):
    return COMPANY_SIZES.get(normalize(company), '')

def days_ago(date_str):
    """Retourne l'ancienneté en jours depuis date_str (ISO ou YYYY-MM-DD)."""
    try:
        if not date_str:
            return 999
        d = date_str[:10]
        dt = datetime.strptime(d, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        return (NOW - dt).days
    except Exception:
        return 999


# ─────────────────────────────────────────────────────────────────────────────
# FRANCE TRAVAIL API
# ─────────────────────────────────────────────────────────────────────────────

def ft_get_token():
    if not FT_CLIENT_ID or not FT_CLIENT_SECRET:
        print('  [FT] Pas de credentials — skip')
        return None
    url  = 'https://entreprise.francetravail.fr/connexion/oauth2/access_token?realm=%2Fpartenaire'
    data = urllib.parse.urlencode({
        'grant_type': 'client_credentials',
        'client_id': FT_CLIENT_ID,
        'client_secret': FT_CLIENT_SECRET,
        'scope': 'api_offresdemploiv2',
    }).encode()
    req = urllib.request.Request(url, data=data, method='POST')
    req.add_header('Content-Type', 'application/x-www-form-urlencoded')
    try:
        resp = urllib.request.urlopen(req, context=ctx, timeout=15)
        return json.loads(resp.read())['access_token']
    except urllib.error.HTTPError as e:
        body = e.read().decode('utf-8', errors='replace')
        print(f'  [FT] Token error HTTP {e.code}: {body[:300]}')
        return None
    except Exception as e:
        print(f'  [FT] Token error: {e}')
        return None

def ft_search(token, query, max_results=150):
    """Recherche France Travail par mot-clé."""
    base = 'https://api.francetravail.io/partenaire/offresdemploi/v2/offres/search'
    params = {
        'motsCles': query,
        'typeContrat': 'CDI',
        'range': f'0-{min(max_results-1, 149)}',
    }
    url = base + '?' + urllib.parse.urlencode(params)
    req = urllib.request.Request(url)
    req.add_header('Authorization', f'Bearer {token}')
    req.add_header('Accept', 'application/json')
    try:
        resp = urllib.request.urlopen(req, context=ctx, timeout=20)
        body = resp.read()
        if not body or resp.status == 204:
            return []  # Aucun résultat
        data = json.loads(body)
        return data.get('resultats', [])
    except urllib.error.HTTPError as e:
        if e.code in (204, 400, 404):
            return []  # Pas de résultats pour cette requête
        print(f'  [FT] Search "{query}" HTTP {e.code}')
        return []
    except Exception as e:
        print(f'  [FT] Search "{query}" error: {e}')
        return []

# Requêtes ciblées BTP/Immo pour France Travail
FT_QUERIES = [
    'promoteur immobilier directeur programmes',
    'conducteur travaux chef chantier',
    'asset manager property manager immobilier',
    'négociateur agent immobilier transaction',
    'foncier développement foncier acquisition',
    'maître œuvre MOE MOA ingénieur bâtiment',
    'bureau études structure génie civil BTP',
    'architecte urbanisme conception',
    'expert immobilier évaluation',
    'gestionnaire immobilier patrimoine locatif',
    'juriste droit immobilier construction',
    'investissement immobilier SCPI financement',
]

def fetch_france_travail():
    print('\n[France Travail] Récupération des offres BTP/Immo...')
    token = ft_get_token()
    if not token:
        return []

    seen, jobs = set(), []

    def fetch_query(q):
        results = ft_search(token, q)
        time.sleep(0.5)
        return results

    with ThreadPoolExecutor(max_workers=3) as ex:
        all_results = list(ex.map(fetch_query, FT_QUERIES))

    for results in all_results:
        for r in results:
            jid = r.get('id', '')
            if jid in seen:
                continue
            seen.add(jid)

            title   = r.get('intitule', '')
            company = (r.get('entreprise') or {}).get('nom', '') or ''
            location = (r.get('lieuTravail') or {}).get('libelle', '') or ''
            url      = (r.get('origineOffre') or {}).get('urlOrigine', '') or (
                        f'https://www.francetravail.fr/offres/recherche/detail/{jid}')
            date_str = (r.get('dateCreation') or '')[:10]
            desc     = r.get('description', '')

            # Filtre pertinence : doit avoir un mot-clé du secteur dans le titre
            title_lower = normalize(title)
            relevant = any(
                kw in title_lower
                for kw in [
                    'immobilier', 'foncier', 'btp', 'bâtiment', 'construction',
                    'travaux', 'chantier', 'promoteur', 'promotion',
                    'asset', 'property', 'facilities', 'facility',
                    'négociateur', 'agent', 'transaction', 'gérance',
                    'architecte', 'urbanis', 'bureau d\'études', 'structure',
                    'génie civil', 'maître d\'œuvre', 'moe', 'moa', 'amo',
                    'géotechnique', 'vrd', 'expert', 'évaluateur', 'patrimoine',
                    'juriste', 'notaire', 'investissement', 'scpi',
                    'conducteur', 'chef de chantier', 'ingénieur travaux',
                ]
            )
            if not relevant:
                continue

            jobs.append({
                'id':           f'ft_{jid}',
                'title':        title,
                'company':      company,
                'location':     _clean_location(location),
                'url':          url,
                'source':       'France Travail',
                'date':         date_str,
                'days_old':     days_ago(date_str),
                'category':     classify_category(title, desc[:500]),
                'size':         get_company_size(company),
                'is_recruiter': is_recruitment_firm(company),
                'description':  desc[:300],
            })

    print(f'  → {len(jobs)} offres France Travail (BTP/Immo)')
    return jobs


# ─────────────────────────────────────────────────────────────────────────────
# APEC
# ─────────────────────────────────────────────────────────────────────────────

def apec_get_token():
    if not APEC_EMAIL or not APEC_PASSWORD:
        print('  [APEC] Pas de credentials — skip')
        return None
    url  = 'https://authentification-candidat.apec.fr/cas/v1/tickets'
    data = urllib.parse.urlencode({'username': APEC_EMAIL, 'password': APEC_PASSWORD}).encode()
    req  = urllib.request.Request(url, data=data, method='POST')
    req.add_header('Content-Type', 'application/x-www-form-urlencoded')
    try:
        resp = urllib.request.urlopen(req, context=ctx, timeout=15)
        loc  = resp.headers.get('Location', '')
        tgt  = loc.split('/')[-1] if loc else ''
        if not tgt:
            print('  [APEC] Auth échouée : pas de TGT dans la réponse')
            return None
        # Récupère le ST
        url2  = f'https://authentification-candidat.apec.fr/cas/v1/tickets/{tgt}'
        data2 = urllib.parse.urlencode({'service': 'https://www.apec.fr'}).encode()
        req2  = urllib.request.Request(url2, data=data2, method='POST')
        req2.add_header('Content-Type', 'application/x-www-form-urlencoded')
        resp2 = urllib.request.urlopen(req2, context=ctx, timeout=15)
        return resp2.read().decode('utf-8').strip()
    except Exception as e:
        print(f'  [APEC] Auth error: {e}')
        return None

APEC_QUERIES = [
    'immobilier', 'BTP', 'construction', 'foncier',
    'asset manager', 'conducteur travaux', 'maître oeuvre',
    'promoteur immobilier', 'property management',
]

def fetch_apec():
    print('\n[APEC] Récupération des offres cadres BTP/Immo...')
    token = apec_get_token()
    if not token:
        return []

    seen, jobs = set(), []
    base = 'https://www.apec.fr/cms/webservices/rechercherOffre/parCriteres'

    for q in APEC_QUERIES:
        params = urllib.parse.urlencode({
            'motsCles': q,
            'typeContrat': 'CDI',
            'nbreOffresParPage': 50,
            'numeroPage': 0,
        })
        url = base + '?' + params
        req = urllib.request.Request(url)
        req.add_header('kerlAuthentification', token)
        req.add_header('Accept', 'application/json')
        try:
            resp = urllib.request.urlopen(req, context=ctx, timeout=20)
            data = json.loads(resp.read())
            results = data.get('resultats', [])
            for r in results:
                jid = str(r.get('numeroOffre') or r.get('id') or '')
                if jid in seen:
                    continue
                seen.add(jid)
                title   = r.get('intitulePoste') or r.get('title', '')
                company = r.get('nomEmployeur') or r.get('company', '')
                loc     = r.get('localisation') or r.get('location', '')
                if isinstance(loc, dict):
                    loc = loc.get('libelle', '')
                date_str = (r.get('dateCreation') or '')[:10]
                jobs.append({
                    'id':           f'apec_{jid}',
                    'title':        title,
                    'company':      company,
                    'location':     _clean_location(loc),
                    'url':          f'https://www.apec.fr/candidat/recherche-emploi.html/emploi/detail-offre/{jid}',
                    'source':       'APEC',
                    'date':         date_str,
                    'days_old':     days_ago(date_str),
                    'category':     classify_category(title),
                    'size':         get_company_size(company),
                    'is_recruiter': is_recruitment_firm(company),
                    'description':  '',
                })
        except Exception as e:
            print(f'  [APEC] Query "{q}" error: {e}')
        time.sleep(0.3)

    print(f'  → {len(jobs)} offres APEC')
    return jobs


# ─────────────────────────────────────────────────────────────────────────────
# HELLOWORK (scraping HTML — pas d'API publique)
# ─────────────────────────────────────────────────────────────────────────────

import html as _html_mod

HW_SECTORS = ['BTP', 'Immo']  # codes secteurs HelloWork
HW_MAX_PAGES = 5              # ~30 offres/page → jusqu'à 150 offres/secteur

def hw_relative_to_date(text):
    """Convertit 'il y a 14 heures' / 'il y a 3 jours' / 'Aujourd'hui' en date ISO."""
    text = (text or '').strip().lower()
    if not text:
        return NOW.strftime('%Y-%m-%d')
    if 'aujourd' in text:
        return NOW.strftime('%Y-%m-%d')
    if 'hier' in text:
        return (NOW - timedelta(days=1)).strftime('%Y-%m-%d')
    m = re.search(r'(\d+)\s*heure', text)
    if m:
        return NOW.strftime('%Y-%m-%d')
    m = re.search(r'(\d+)\s*jour', text)
    if m:
        return (NOW - timedelta(days=int(m.group(1)))).strftime('%Y-%m-%d')
    m = re.search(r'(\d+)\s*semaine', text)
    if m:
        return (NOW - timedelta(weeks=int(m.group(1)))).strftime('%Y-%m-%d')
    m = re.search(r'(\d+)\s*mois', text)
    if m:
        return (NOW - timedelta(days=int(m.group(1)) * 30)).strftime('%Y-%m-%d')
    return NOW.strftime('%Y-%m-%d')

def hw_fetch_page(sector, page):
    params = {
        'k': '', 'l': '', 'c': 'CDI', 'd': 'all',
        'et': 'Entreprises', 's': sector, 'p': page,
    }
    url = 'https://www.hellowork.com/fr-fr/emploi/recherche.html?' + urllib.parse.urlencode(params)
    req = urllib.request.Request(url)
    req.add_header('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                                  '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    try:
        resp = urllib.request.urlopen(req, context=ctx, timeout=20)
        return resp.read().decode('utf-8', errors='replace')
    except Exception as e:
        print(f'  [HelloWork] Page {page} ({sector}) error: {e}')
        return ''

def hw_parse_jobs(html_text):
    jobs = []
    blocks = re.split(r'(?=<li data-id-storage-target="item")', html_text)
    for block in blocks[1:]:
        m_id = re.search(r'data-id-storage-item-id="(\d+)"', block)
        if not m_id:
            continue
        jid = m_id.group(1)

        m_anchor = re.search(r'<a\b[^>]*?data-cy="offerTitle"[^>]*>', block, re.S)
        if not m_anchor:
            continue
        m_title = re.search(r'title="([^"]+)"', m_anchor.group(0))
        if not m_title:
            continue
        title = _html_mod.unescape(m_title.group(1).strip())

        m_company = re.search(r'<p class="typo-s inline">([^<]*)</p>', block)
        company = _html_mod.unescape(m_company.group(1).strip()) if m_company else ''

        # Le title="" contient "Titre - ... - Entreprise" → on retire le suffixe entreprise dupliqué
        if company and title.endswith(company):
            title = title[:-len(company)].rstrip(' -–')

        m_loc = re.search(r'data-cy="localisationCard"\s*>\s*([^<]+?)\s*</div>', block, re.S)
        location = _html_mod.unescape(m_loc.group(1).strip()) if m_loc else ''

        m_date = re.search(r'text-grey-500[^>]*>\s*([^<]+?)\s*</div>', block, re.S)
        date_relative = m_date.group(1).strip() if m_date else ''

        jobs.append({
            'jid': jid,
            'title': title,
            'company': company,
            'location': location,
            'date_str': hw_relative_to_date(date_relative),
        })
    return jobs

def fetch_hellowork():
    print('\n[HelloWork] Récupération des offres BTP/Immo...')
    seen, jobs = set(), []

    def fetch_sector_page(args):
        sector, page = args
        html_text = hw_fetch_page(sector, page)
        time.sleep(0.4)
        return hw_parse_jobs(html_text) if html_text else []

    tasks = [(sector, page) for sector in HW_SECTORS for page in range(1, HW_MAX_PAGES + 1)]
    with ThreadPoolExecutor(max_workers=4) as ex:
        all_results = list(ex.map(fetch_sector_page, tasks))

    excluded_kw = ['développeur', 'developer', 'data scientist', 'devops', 'ux designer',
                   'ui designer', 'community manager', 'social media']

    for results in all_results:
        for r in results:
            if r['jid'] in seen:
                continue
            seen.add(r['jid'])
            if any(kw in normalize(r['title']) for kw in excluded_kw):
                continue
            jobs.append({
                'id':           f'hw_{r["jid"]}',
                'title':        r['title'],
                'company':      r['company'],
                'location':     _clean_location(r['location']),
                'url':          f'https://www.hellowork.com/fr-fr/emplois/{r["jid"]}.html',
                'source':       'HelloWork',
                'date':         r['date_str'],
                'days_old':     days_ago(r['date_str']),
                'category':     classify_category(r['title']),
                'size':         get_company_size(r['company']),
                'is_recruiter': is_recruitment_firm(r['company']),
                'description':  '',
            })

    print(f'  → {len(jobs)} offres HelloWork')
    return jobs


# ─────────────────────────────────────────────────────────────────────────────
# LOGOS ENTREPRISES (via Clearbit autocomplete — gratuit, sans clé)
# ─────────────────────────────────────────────────────────────────────────────

_domain_cache = {}

def get_company_domain(company):
    key = normalize(company)
    if not key:
        return None
    if key in _domain_cache:
        return _domain_cache[key]
    domain = None
    try:
        url = ('https://autocomplete.clearbit.com/v1/companies/suggest?' +
               urllib.parse.urlencode({'query': company}))
        req = urllib.request.Request(url)
        resp = urllib.request.urlopen(req, context=ctx, timeout=6)
        data = json.loads(resp.read())
        if data:
            domain = data[0].get('domain')
    except Exception:
        domain = None
    _domain_cache[key] = domain
    return domain

def enrich_with_logos(jobs):
    companies = {}
    for j in jobs:
        key = normalize(j['company'])
        if key and key not in companies:
            companies[key] = j['company']

    with ThreadPoolExecutor(max_workers=10) as ex:
        list(ex.map(get_company_domain, companies.values()))

    found = sum(1 for v in _domain_cache.values() if v)
    print(f'  {found}/{len(companies)} logos résolus')

    for j in jobs:
        domain = _domain_cache.get(normalize(j['company']))
        j['logo'] = f'https://www.google.com/s2/favicons?sz=128&domain={domain}' if domain else ''
    return jobs


# ─────────────────────────────────────────────────────────────────────────────
# UTILITAIRES
# ─────────────────────────────────────────────────────────────────────────────

LOCATION_MAP = {
    '75': 'Paris', '77': 'Île-de-France', '78': 'Île-de-France',
    '91': 'Île-de-France', '92': 'Île-de-France', '93': 'Île-de-France',
    '94': 'Île-de-France', '95': 'Île-de-France',
    '69': 'Lyon', '13': 'Marseille', '33': 'Bordeaux',
    '31': 'Toulouse', '67': 'Strasbourg', '44': 'Nantes',
    '35': 'Rennes', '59': 'Lille', '06': 'Nice', '34': 'Montpellier',
}

def _clean_location(loc):
    if not loc:
        return ''
    loc = loc.strip()
    # "75056 - PARIS" → "Paris"
    m = re.match(r'^(\d{2})\d{3}\s*[-–]\s*(.+)$', loc)
    if m:
        dept, city = m.group(1), m.group(2).title()
        return city
    # "75 - Paris" style
    m2 = re.match(r'^(\d{2})\s*[-–]\s*(.+)$', loc)
    if m2:
        return m2.group(2).title()
    # "Paris (75)" style
    m3 = re.match(r'^(.+?)\s*\(\d+\)', loc)
    if m3:
        return m3.group(1).strip()
    # Télétravail
    if re.search(r't[eé]l[eé]travail|remote|full.?remote', loc, re.I):
        return 'Télétravail'
    return loc[:50]


# ─────────────────────────────────────────────────────────────────────────────
# CRM ENRICHMENT
# ─────────────────────────────────────────────────────────────────────────────

def load_crm_cache():
    if not os.path.exists(CRM_CACHE):
        return {}
    try:
        with open(CRM_CACHE, encoding='utf-8') as f:
            data = json.load(f)
        return data.get('companies', {})
    except Exception:
        return {}

COMPANY_NOISE_WORDS = {
    'groupe', 'group', 'sas', 'sasu', 'sarl', 'sa', 'eurl', 'sci',
    'holding', 'france', 'international', 'sas.', 'sarl.',
}

def strip_company_suffixes(name):
    """Retire les mots génériques ('Groupe', 'SAS'...) pour matcher 'Groupe Snef' ↔ 'Snef'."""
    words = [w for w in re.split(r'[\s\-]+', name) if w and w not in COMPANY_NOISE_WORDS]
    return ' '.join(words).strip()

def enrich_with_crm(jobs, crm):
    if not crm:
        return jobs

    # Index secondaire : nom "nettoyé" (sans Groupe/SAS/etc.) → données CRM
    crm_clean = {}
    for crm_key, co_data in crm.items():
        clean = strip_company_suffixes(crm_key)
        if clean and clean not in crm_clean:
            crm_clean[clean] = co_data

    enriched = 0
    for j in jobs:
        key = normalize(j['company'])
        co  = crm.get(key) or crm_clean.get(strip_company_suffixes(key))
        if co:
            j['crm_link']      = co.get('crm_link', '')
            j['crm_status']    = co.get('status', '')
            j['has_tc']        = co.get('has_tc', False)
            j['has_open_job']  = co.get('has_open_job', False)
            j['is_client']     = co.get('is_client', False)
            j['consultant']    = co.get('consultant', '')
            j['updated_by']    = co.get('updated_by', '')
            j['updated_on']    = co.get('updated_on', '')
            enriched += 1
        else:
            j['crm_link']     = ''
            j['crm_status']   = ''
            j['has_tc']       = False
            j['has_open_job'] = False
            j['is_client']    = False
            j['consultant']   = ''
            j['updated_by']   = ''
            j['updated_on']   = ''
    print(f'  CRM enrichissement : {enriched}/{len(jobs)} entreprises matchées')
    return jobs


# ─────────────────────────────────────────────────────────────────────────────
# GÉNÉRATION HTML
# ─────────────────────────────────────────────────────────────────────────────

def generate_html(jobs):
    if not os.path.exists(TEMPLATE):
        raise FileNotFoundError(f'template.html introuvable : {TEMPLATE}')

    with open(TEMPLATE, encoding='utf-8') as f:
        html = f.read()

    jobs_json = json.dumps(jobs, ensure_ascii=False, separators=(',', ':'))
    jobs_json = jobs_json.replace('</', '<\\/')  # évite </script> dans les descriptions
    meta_json = json.dumps({
        'updated': NOW.strftime('%d/%m/%Y %H:%M UTC'),
        'total':   len(jobs),
    }, ensure_ascii=False)

    html = html.replace('/* __JOBS_JSON__ */', jobs_json)
    html = html.replace('/* __META_JSON__ */', meta_json)

    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f'\nHTML généré : {OUT}')
    print(f'  {len(jobs)} offres — mis à jour le {NOW.strftime("%d/%m/%Y %H:%M UTC")}')


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print('=' * 60)
    print('JobRadar Briks — Immobilier / Construction / BTP')
    print(f'Démarrage : {NOW.strftime("%d/%m/%Y %H:%M UTC")}')
    print('=' * 60)

    # 1. Scraping (en parallèle)
    with ThreadPoolExecutor(max_workers=3) as ex:
        ft_future   = ex.submit(fetch_france_travail)
        apec_future = ex.submit(fetch_apec)
        hw_future   = ex.submit(fetch_hellowork)
        ft_jobs     = ft_future.result()
        apec_jobs   = apec_future.result()
        hw_jobs     = hw_future.result()

    all_jobs = ft_jobs + apec_jobs + hw_jobs

    # 2. Dédoublonnage par titre + entreprise
    seen_keys = set()
    deduped = []
    for j in all_jobs:
        key = normalize(j['title']) + '|' + normalize(j['company'])
        if key not in seen_keys:
            seen_keys.add(key)
            deduped.append(j)
    print(f'\nTotal après dédup : {len(deduped)} offres (sur {len(all_jobs)} brutes)')

    # 3. CRM enrichissement
    print('\n[CRM] Chargement du cache RecruitCRM...')
    crm = load_crm_cache()
    print(f'  {len(crm)} entreprises en cache')
    deduped = enrich_with_crm(deduped, crm)

    # 3b. Logos entreprises
    print('\n[Logos] Résolution des domaines entreprises...')
    deduped = enrich_with_logos(deduped)

    # 4. Tri : T&Cs d'abord, puis par date
    deduped.sort(key=lambda j: (
        not j.get('has_tc', False),
        j.get('days_old', 999),
    ))

    # 5. Génération HTML
    generate_html(deduped)
    print('\nTerminé.')

if __name__ == '__main__':
    main()
