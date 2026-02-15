#!/usr/bin/env python3
import urllib.request, json, time
owner='ndorotate'
repo='ipc-rpc-parallel-computing-mhofu-dot'
url=f'https://api.github.com/repos/{owner}/{repo}/actions/runs'
print('Starting to poll Actions API for', owner+'/'+repo)
start=time.time()
while True:
    try:
        resp=urllib.request.urlopen(url, timeout=15)
        data=resp.read().decode()
        j=json.loads(data)
        runs=j.get('workflow_runs',[])
        if not runs:
            print(time.strftime('%Y-%m-%d %H:%M:%S'), 'no_runs')
        else:
            latest=runs[0]
            rid=latest.get('id')
            name=latest.get('name')
            event=latest.get('event')
            status=latest.get('status')
            conclusion=latest.get('conclusion')
            print(time.strftime('%Y-%m-%d %H:%M:%S'), f'id={rid} name="{name}" event={event} status={status} conclusion={conclusion}')
            if status=='completed':
                print('Workflow run completed; exiting poll loop')
                break
    except Exception as e:
        print(time.strftime('%Y-%m-%d %H:%M:%S'), 'error:', e)
    time.sleep(30)
print('Total poll time (s):', int(time.time()-start))
