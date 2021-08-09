# -*- coding: utf-8 -*-
"""
Created on Sat Aug  7 02:57:12 2021

@author: apple
"""

import pandas as pd
import sqlite3
import codecs
import re
import numpy as np
import dedupe
from sqlalchemy import create_engine

def loader():
    conn = sqlite3.connect('test3.db', timeout = 2)
    with codecs.open('C:\\Users\\apple\\Desktop\\repos\\telegram_bot\\outlets2.sql', 'r', 'utf-8') as f:
        text = f.read()
    queries = text.split(';\n')
    queries = [query.replace('\n', '') for query in queries]
    cur = conn.cursor()
    for query in queries:
        cur.execute(query)
    outlets = pd.read_sql('select * from outlets', conn)
    outlets_clean = pd.read_sql('select * from outlets_clean', conn)
    return outlets
def initials(text):
    t = re.findall(r'([А-ЯЁ])[., ]*([А-ЯЁ])', text)
    return ''.join(t[0])
def standart_spot(spot):
    if spot.startswith('.'):
        spot = spot[1:]
    if 'ИП' in spot:
        t = re.findall(r'([ИП]{2})?[ ]?([А-ЯЁёа-я]+) ([А-ЯЁ][., ]+[А-ЯЁ][.,]*)[ ]?([ИП]{2})?(.*)', spot)
        if t:
            text = list(t[0])
            if 'ИП' in text:
                ind = text.index('ИП')
                text.pop(ind)
            if '' in text:
                ind = text.index('')
                text.pop(ind)
            init = initials(text[1])
            return [f'{text[0]} {init}', text[2], 'ИП']
            
        t = re.findall(r'([ИП]{2})?[ ]?([А-ЯЁёа-я]+)[ ]?([ИП]{2})?(.*)', spot)
        if t:
            text = list(t[0])
            if 'ИП' in text:
                ind = text.index('ИП')
                text.pop(ind)
            if '' in text:
                ind = text.index('')
                text.pop(ind)
            return [f'{text[0]}', text[1], 'ИП']
    if 'ООО' in spot:
        spot = spot.replace('\\', '')
        t = re.findall(r'^[\"]*(([О]{3})[ ]?[.А-Яа-яёЁ№0-9\\\"\-\+ ]+)(.*)', spot)
        if t:
            text = list(t[0])
            
            if 'ООО' in text:
                ind = text.index('ООО')
                text.pop(ind)
            return [text[0], text[1], 'ООО']
        t = re.findall(r'^([.А-Яа-яёЁ№0-9\\\\"\-\+ ]+) [.]?([О]{3})[ ]?[.\\\'\"]*(.*)', spot)
        if t:
            text = list(t[0])
            if 'ООО' in text:
                ind = text.index('ООО')
                text.pop(ind)
            return [text[0], text[1], 'ООО']
        else:
            return [spot, '', '']
    else:
        return [spot.replace('.', ''), '', '']
def foo(x):
    if x != 'он же' and x != '-':
        return x
    else: return np.nan

    
def learning(not_nan):
    variables = [{'field' : 'name', 'type': 'String'},
                 {'field' : 'address_fin', 'type': 'String'},
                 ]
    deduper = dedupe.Dedupe(variables)
    data = not_nan[['name', 'address_fin']].to_dict(orient='index')
    with open('training.json') as f:
        deduper.prepare_training(data = data, training_file = f, sample_size = 1000, blocked_proportion = .5)
    dedupe.convenience.console_label(deduper)
    deduper.train(recall=0.9)
    clustered_dupes = deduper.partition(data, threshold=0.3)
    return clustered_dupes



def main():
    outlets = loader()
    outlets_init = outlets.copy() 
    outlets['standart'] = outlets['Торг_точка_грязная'].apply(standart_spot)
    outlets[['name', 'etc', 'type']] = pd.DataFrame(outlets.standart.tolist(), index=outlets.index)
    outlets['address'] = outlets['Торг_точка_грязная_адрес'].map(foo)
    outlets['address_fin'] = outlets['address'].combine_first(outlets['etc'])
    outlets_v2= outlets.drop(['Торг_точка_грязная_адрес', 'address', 'Торг_точка_грязная', 'etc', 'standart'], axis = 1)
    not_nan = outlets_v2[outlets_v2['address_fin'] != '']
    nan_val = outlets_v2[outlets_v2['address_fin'] == '']
    column = {}
    for i in list(nan_val.index):
        column[i] = np.nan
    clustered_dupes = learning(not_nan)
    count = 1
    for i in [group[0] for group in clustered_dupes]:
        for x in i:
            column[x] = count
        count +=1
    sorted_ = sorted(list(column.items()))
    outlets_init['outlet_clean_id'] = [i[1] for i in sorted_]
    return outlets_init

    
    
if __name__ == '__main__':
    outlets = main()
