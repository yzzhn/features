import pandas as pd

def honeypot_match(zeek_df, honeypot):
    df1 = pd.merge(zeek_df, honeypot, left_on='id_orig_h', right_on='indicator')
    df1['id_orig_h_inHoneypot'] = 1
    df2 = pd.merge(zeek_df, honeypot, left_on='id_resp_h', right_on='indicator')
    df2['id_resp_h_inHoneypot'] = 1
    res = pd.concat([df1, df2], sort=False, ignore_index=True)
    res[['id_orig_h_inHoneypot', 'id_resp_h_inHoneypot']] = res[['id_orig_h_inHoneypot', 'id_resp_h_inHoneypot']].fillna(0)
    return res
