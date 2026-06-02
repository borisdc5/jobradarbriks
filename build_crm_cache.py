"""
Weekly job: fetches all companies + all jobs from RecruitCRM and writes crm_cache.json.
Run via GitHub Actions every Sunday, or manually:
  RECRUITCRM_API_KEY=xxx python3 build_crm_cache.py
"""
import urllib.request, urllib.parse, ssl, json, os, time
from datetime import datetime, timezone

TOKEN = os.getenv('RECRUITCRM_API_KEY', '')
BASE  = 'https://api.recruitcrm.io/v1'
RCRM_APP = 'https://app.recruitcrm.io/v1/company'
OUT   = os.path.join(os.path.dirname(__file__), 'crm_cache.json')

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode    = ssl.CERT_NONE

def rcrm_get(path, params=None):
    url = BASE + path
    if params:
        url += '?' + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={
        'Authorization': f'Bearer {TOKEN}',
        'Accept': 'application/json',
        'User-Agent': 'JobRadar-Briks/1.0',
    })
    try:
        resp = urllib.request.urlopen(req, context=ctx, timeout=15)
        return json.loads(resp.read())
    except Exception as e:
        print(f'  Erreur API: {e}')
        return None

if not TOKEN:
    print('RECRUITCRM_API_KEY absent — abandon')
    exit(1)

# ── 1. Fetch all companies ────────────────────────────────────────────────────
print('Chargement de toutes les entreprises RecruitCRM...')
companies = {}
page = 1
consecutive_errors = 0

while True:
    data = rcrm_get('/companies', {'per_page': 100, 'page': page})
    if not data:
        consecutive_errors += 1
        if consecutive_errors >= 3:
            print(f'  3 erreurs consécutives — arrêt à la page {page}')
            break
        time.sleep(2)
        continue
    consecutive_errors = 0

    items = data.get('data', [])
    if not items:
        break

    if page == 1 and items:
        print(f'  [debug] Champs bruts API company: {sorted(items[0].keys())}')

    for c in items:
        name = (c.get('company_name') or '').strip()
        if not name:
            continue
        slug = c.get('slug') or str(c.get('id') or '')
        crm_link = f'{RCRM_APP}/{slug}' if slug else ''

        status = ''
        for cf in c.get('custom_fields', []):
            if cf.get('field_name') == 'Company Status':
                status = (cf.get('value') or '').strip()
                break

        updated_by_id = (c.get('updated_by') or c.get('last_modified_by')
                         or c.get('modified_by') or c.get('last_updated_by'))

        companies[name.lower()] = {
            'company_name':   name,
            'crm_link':       crm_link,
            'status':         status,
            'is_client':      status == 'Active Account',
            'slug':           slug,
            'numeric_id':     c.get('id'),
            'owner_id':       c.get('owner'),
            'updated_on':     c.get('updated_on') or '',
            'updated_by_id':  updated_by_id,
        }

    print(f'  Page {page:3d} → {len(items)} entrées (total {len(companies)})')

    if len(items) < 100:
        break

    page += 1
    time.sleep(1.1)

slug_to_key = {v['slug']: k for k, v in companies.items() if v['slug']}

# ── 2. Fetch all jobs ─────────────────────────────────────────────────────────
print('\nChargement de tous les jobs RecruitCRM...')
job_map = {}
total_jobs = 0
page_num = 0
next_url = f'{BASE}/jobs?per_page=100'
MAX_JOB_PAGES = 600
no_new_streak = 0

while next_url and page_num < MAX_JOB_PAGES:
    req = urllib.request.Request(next_url, headers={
        'Authorization': f'Bearer {TOKEN}',
        'Accept': 'application/json',
        'User-Agent': 'JobRadar-Briks/1.0',
    })
    try:
        resp = urllib.request.urlopen(req, context=ctx, timeout=15)
        data = json.loads(resp.read())
    except Exception as e:
        print(f'  Erreur jobs page {page_num+1}: {e}')
        break

    items = data.get('data', [])
    if not items:
        break

    new_slugs = 0
    for j in items:
        co_slug = j.get('company_slug', '')
        if not co_slug:
            continue
        is_open = (j.get('job_status') or {}).get('label') == 'Open'
        if co_slug not in job_map:
            job_map[co_slug] = {'has_open': False, 'owner_id': None}
            new_slugs += 1
        if is_open and not job_map[co_slug]['has_open']:
            job_map[co_slug]['has_open'] = True
            job_map[co_slug]['owner_id'] = j.get('owner')

    total_jobs += len(items)
    page_num += 1
    print(f'  Page {page_num:3d} → {len(items)} jobs (total {total_jobs}, slugs uniques: {len(job_map)})')

    if new_slugs == 0:
        no_new_streak += 1
        if no_new_streak >= 20:
            print(f'  20 pages sans nouvelle entreprise — arrêt anticipé')
            break
    else:
        no_new_streak = 0

    next_url = data.get('next_page_url')
    if next_url:
        time.sleep(1.1)

# ── 3. Fetch users (consultants) ──────────────────────────────────────────────
print('\nChargement des consultants (users)...')
users = {}
udata = rcrm_get('/users?per_page=200')
if udata:
    ulist = udata if isinstance(udata, list) else udata.get('data', [])
    for u in ulist:
        uid = u.get('id')
        fname = (u.get('first_name') or '').strip()
        lname = (u.get('last_name') or '').strip()
        uname = f'{fname} {lname}'.strip()
        if uid and uname:
            users[str(uid)] = uname
    print(f'  {len(users)} consultants chargés')

# ── 4. Enrich companies with job data ─────────────────────────────────────────
print('\nEnrichissement des entreprises avec données jobs...')
for company in companies.values():
    slug = company['slug']
    jd = job_map.get(slug)
    company['has_tc']       = jd is not None
    company['has_open_job'] = jd['has_open'] if jd else False
    company['is_client']    = company.get('status') == 'Active Account' and company['has_open_job']
    consultant_id = (jd['owner_id'] if jd else None) or company.get('owner_id')
    company['consultant'] = users.get(str(consultant_id), '') if consultant_id else ''
    upd_id = company.pop('updated_by_id', None)
    company['updated_by'] = users.get(str(upd_id), '') if upd_id else ''

with_tc   = sum(1 for v in companies.values() if v['has_tc'])
with_open = sum(1 for v in companies.values() if v['has_open_job'])
print(f'  {with_tc} entreprises avec T&Cs, {with_open} avec job ouvert')

# ── 5. Write cache ────────────────────────────────────────────────────────────
cache = {
    'updated':   datetime.now(timezone.utc).isoformat(),
    'total':     len(companies),
    'companies': companies,
    'users':     users,
}

with open(OUT, 'w', encoding='utf-8') as f:
    json.dump(cache, f, ensure_ascii=False, separators=(',', ':'))

clients  = sum(1 for v in companies.values() if v['is_client'])
prospect = sum(1 for v in companies.values() if v['status'] == 'Prospect')
print(f'\nCache écrit : {len(companies)} entreprises '
      f'({clients} clients actifs, {prospect} prospects)')
print(f'  {with_tc} T&Cs signés, {with_open} jobs ouverts')
print(f'Fichier : {OUT}')
