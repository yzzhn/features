import os
import sys
import pandas as pd
import numpy as np
import datetime
import scipy as sp
from scipy import signal, stats
import multiprocessing

### compute periodogram of the signal
def compute_psd(data, sample_freq=1):
    freq, pxx_den = signal.periodogram(data, sample_freq)
    return freq, pxx_den

### random permute data and find PSD threshold as the (permute_cnt * Confidence)-th Maximum PSD value
#def get_psd_threshold(data, permute_cnt=20, C=0.95, sample_freq=1):
def get_psd_threshold(data, permute_cnt=60, C=0.95, sample_freq=1):
    max_psd = []
    for i in range(permute_cnt):
        permuted_data = np.random.permutation(data)
        _, t_psd = compute_psd(permuted_data, sample_freq)
        max_power = np.max(t_psd)
        max_psd.append(max_power)
        
    rank = int(C * permute_cnt) - 1
    max_psd.sort()
    psd_threshold = max_psd[rank]
    return psd_threshold

### filter the freq using psd_threshold
### then convert freq to periods
def get_potential_periods(freq, psd, psd_threshold):
    freq = np.array(freq)
    psd = np.array(psd)
    res = 1 / freq[psd>psd_threshold]
    return res[res<720]

### get time intervals between connections
def get_ts_intervals(data):
    data_idx = np.arange(len(data))
    data = np.array(data)
    ts_intervals = np.diff(data_idx[data>0])
    return ts_intervals

### get the minimum time intervals
def get_min_tsinterval(ts_intervals):
    if len(ts_intervals) > 0:
        return min(ts_intervals)
    return 0

### filter potential periods
def high_freq_pruning(potential_periods, min_tsintveral):
    return potential_periods[potential_periods>=min_tsintveral]

### max psd / psd_threshold 
### TODO:
def psd_ratio(psd, threshold):
    return max(psd) / threshold

### CAUTION:
#### pvalue_pruning will cause pattern periodicity lost
def pvalue_pruning(tsintervals, potential_freqs, sigma=10, alpha=0.05):
    res = []
    for period in potential_freqs:
        rvs = stats.norm.rvs(loc=period, scale=sigma, size=20)
        _, pvalue = stats.ttest_ind(rvs, tsintervals)
        if pvalue > alpha:
            res.append(period)
    return res

### ACF of orig signal
def autocorr(x):
    result = np.correlate(x, x, mode='full')
    result = result[result.size//2:]
    result[0] = 0
    return result

### get the peaks of autocorr
def get_autocorr_peaks(data):
    autocor_ts = autocorr(data)
    autocor_ts_norm = autocor_ts / max(autocor_ts)
    #peaks, _ = signal.find_peaks(autocor_ts_norm, height=0.3)
    peaks, _ = signal.find_peaks(autocor_ts_norm, prominence=0.2)
    return peaks

### filter potential periods using autocorr peaks
### if autocorr peaks - 1 < potential_period < autocorr peaks + 1
def true_periodicity(potential_periods, autocorr_peaks, threshold=2):
    true_period = []
    ### TODO: filter the dataframe before calling this func after figuring out pvalue filtering issue
    if len(autocorr_peaks) == 0 or len(potential_periods) == 0:
        return true_period
    
    for period in potential_periods:
        if min(abs(autocorr_peaks - period)) <= threshold:
            true_period.append(period)
    return true_period


def acf_verification(periods, sig):
    res_per = []
    #periods.sort()
    for per in periods:
        #print(pers)
        sos = signal.butter(10, per+1, 'lp', fs=1440, output='sos')
        filtered = signal.sosfilt(sos, sig)
        freq, psd = compute_psd(filtered)
        psd_threshold = get_psd_threshold(filtered)
        potential_periods = 1/freq[psd > psd_threshold]
        potential_periods = potential_periods[potential_periods<720]
        
        autocor_ts = autocorr(filtered) 
        autocor_ts_norm = autocor_ts / max(autocor_ts)
        peaks = get_autocorr_peaks(filtered)
        peaks = peaks[peaks>1]
        
        true_period = true_periodicity(potential_periods, peaks, threshold=2)
        if len(true_period) > 0:
            res_per.append(true_period[0])
    return res_per


def detect_periodicity(df, mute=False):
    
    ## filter ts_cnt > 2
    df = df.loc[df['ts_cnt'] > 2]
    
    if not mute: 
        print("total entries:", df.shape[0])
        
    if df.shape[0] == 0:
        return df  
    
    ## find periodicity hints
    df['freq'], df['psd'] = zip(*df["tdf"].apply(compute_psd))
    df['psd_threshold'] = df["tdf"].apply(get_psd_threshold)
    df["potential_periods"] = df.apply(lambda x: get_potential_periods(x['freq'], x['psd'], x['psd_threshold']), axis=1)
    
    ### filter entries that do not have any potential periods
    df = df.loc[df["potential_periods"].map(len) > 0 ]
    if not mute:
        print("total entries with potential periods:", df.shape[0])
    
    if df.shape[0] == 0:
        return df  
    
    ### filter periods that are high freq noise
    df["ts_intervals"] = df['tdf'].apply(lambda x: get_ts_intervals(x))
    df = df.loc[df["ts_intervals"].map(len) > 0]
    df["min_tsinterval"] = df["ts_intervals"].apply(lambda x: get_min_tsinterval(x))
    df["high_freq_pruned"] = df.apply(lambda x: high_freq_pruning(x['potential_periods'], x['min_tsinterval']), axis=1)
    df = df.loc[df["high_freq_pruned"].map(len) > 0]
    
    if not mute:
        print("total entries after high frequency pruning:", df.shape[0])
    
    if df.shape[0] == 0:
        return df  
    
    df["psd_ratio"] = df.apply(lambda x: psd_ratio(x['psd'], x['psd_threshold']), axis=1)
    
    ### TODO: pvalue pruning might filter out  be weird 
    #df['pvalue_pruned_periods'] = df.apply(lambda x: pvalue_pruning(x['ts_intervals'], x['high_freq_pruned']), axis=1)
    df['autocorr_peaks'] = df.apply(lambda x: get_autocorr_peaks(x['tdf']), axis=1)
    df['true_periods'] = df.apply(lambda x: true_periodicity(x['potential_periods'], x['autocorr_peaks']), axis=1)
    
    df['lowpass_filter_per'] = df.apply(lambda x: acf_verification(x['high_freq_pruned'], x['tdf']), axis=1)
    
    df['freq'] = df['freq'].apply(lambda x: list(x))
    df['psd'] = df['psd'].apply(lambda x: list(x))
    
    return df


def mltproc_detection(df, maxproc = 8, mute = False):
    #nproc = min(round(perdf.shape[0] / 8000), maxproc)
    pool = multiprocessing.Pool(processes = maxproc)
    
    df_lst = np.array_split(df, maxproc)
    res = pool.map(detect_periodicity, df_lst)

    resdf = pd.concat(res)
    return resdf