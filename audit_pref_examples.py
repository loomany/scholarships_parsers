# -*- coding: utf-8 -*-
import json,re
from pathlib import Path

root=Path.cwd()
intl_re = re.compile(r"\b(international student|international students|international applicants?|foreign student|foreign national|non[-\s]?u\.?s\.? citizens?|nonresident|f[-\s]?1 visa|study permit|overseas student|international)\b", re.I)

for src,file,skey in [
 ('scholarships_com','.scholarships_com_prefilter_store.json','item_snapshot'),
 ('bold_org','.bold_prefilter_store.json','item_snapshot'),
 ('bigfuture','.bigfuture_prefilter_store.json','card_row_snapshot')
]:
    p=root/file
    data=json.loads(p.read_text(encoding='utf-8'))
    entries=data.get('entries',{})
    print('\nSOURCE',src,'entries',len(entries))
    shown=0
    for k,e in entries.items():
        if not isinstance(e,dict):
            continue
        snap=e.get(skey) if isinstance(e.get(skey),dict) else {}
        blob=' '.join([
            str(e.get('title') or ''),
            str(e.get('url') or ''),
            json.dumps(snap, ensure_ascii=False)
        ])
        if not intl_re.search(blob):
            continue
        print(json.dumps({
            'title': e.get('title'),
            'status': e.get('prefilter_status'),
            'reason': e.get('prefilter_reason'),
            'url': e.get('url')
        }, ensure_ascii=False))
        shown +=1
        if shown>=15:
            break
    print('shown',shown)
