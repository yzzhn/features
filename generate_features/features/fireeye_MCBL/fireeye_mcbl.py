import pandas as pd
import tldextract

def parse_request(request_text):
    ext = tldextract.extract(request_text)
    fqdn = '.'.join(part for part in ext if part)
    return fqdn
    
def get_fireeye_MCBL(fireeyedf):
    fireeyedf = fireeyedf.loc[fireeyedf['eventName']=='malware-callback']
    fireeyedf['fqdn'] = fireeyedf['request'].apply(lambda x: parse_request(x))
    target_columns = ['request', 'fqdn'] 
    fireeyedf = fireeyedf[target_columns].drop_duplicates()
    
    return fireeyedf
    