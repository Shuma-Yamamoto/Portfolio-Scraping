#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#インポート
import datetime
import urllib.request as req
from bs4 import BeautifulSoup
import joblib
import pandas as pd
from tqdm import tqdm
import boto3

#年月日の設定
year = datetime.date.today().year
month = datetime.date.today().month
day = datetime.date.today().day

print('URLの取得')

#都道府県のリストと、求人サイト上での都道府県の割り当て番号のリストを作成
text = '北海道,青森県,岩手県,宮城県,秋田県,山形県,福島県,茨城県,栃木県,群馬県,埼玉県,千葉県,東京都,神奈川県,新潟県,富山県,石川県,福井県,山梨県,長野県,岐阜県,静岡県,愛知県,三重県,滋賀県,京都府,大阪府,兵庫県,奈良県,和歌山県,鳥取県,島根県,岡山県,広島県,山口県,徳島県,香川県,愛媛県,高知県,福岡県,佐賀県,長崎県,熊本県,大分県,宮崎県,鹿児島県,沖縄県'
tdfk_list = text.split(",")
text = '101,111,109,106,110,108,107,047,046,045,044,043,041,042,117,122,123,124,118,116,085,600,081,084,064,063,061,062,065,066,133,134,132,131,135,138,136,137,139,440,441,442,443,444,445,446,447'
tdfk_num = text.split(",")

#URLの取得を行う関数の定義
def joblib_get_url(x):
    url_list = list()
    tdfk = tdfk_list[x]
    #求人サイトにアクセス
    url = 'https://求人サイト名/検索画面/都道府県の割り当て番号='+tdfk_num[x]
    response = req.urlopen(url)
    parse_html = BeautifulSoup(response,'html.parser')
    #全求人を検索した際のページ数を取得
    page_num = parse_html.find_all('div',class_='pager-number-wrap')[0].find_all('li')[-1].string
    #各ページにアクセス
    for y in range(0,int(page_num)):
        url = 'https://求人サイト名/検索画面/都道府県の割り当て番号='+tdfk_num[x]+'&page='+str(y)
        response = req.urlopen(url)
        parse_html = BeautifulSoup(response,'html.parser')
        #各ページに35件ずつ掲載されている求人のURLを取得
        for z in range(0,35):
            try:
                url_key = parse_html.find_all('div',class_='job-cassette-lst-wrap')[0].find_all('div',class_='job-lst-main-cassette-wrap')[z].find_all('a')[0].attrs['href']
                url_list.append(tdfk)
                url_list.append('https://求人サイト名'+url_key)
            except:
                pass
    return url_list
#以上の処理を都道府県ごとに並列で実行する

#関数の並列処理の実行
all_list = list()
try:
    resultList = joblib.Parallel(n_jobs=-1,verbose=3)([joblib.delayed(joblib_get_url)(x) for x in tqdm(range(0,47))])
    all_list.extend(resultList)
except:
    pass

#リストの平坦化
def flatten(l):
    for el in l:
        if isinstance(el, collections.abc.Iterable) and not isinstance(el, (str, bytes)):
            yield from flatten(el)
        else:
            yield el
all_list = list(flatten(all_list))

#都道府県とURLを１セットとする二次元リストを作成
x = 0
new_all_list = list()
for i in tqdm(all_list):
    new_all_list.append(all_list[x:x+2])
    x += 2
    if x >= len(all_list):
        break

#データフレームに変換
url_df = pd.DataFrame(new_all_list,columns=["（本社住所）都道府県","url"])
url_df = url_df.drop_duplicates(['url']).reset_index(drop=True)

del all_list
del new_all_list

print('求人データの取得')

#求人データの取得を行う関数の定義
def joblib_get_data(i):
    new_list = list()
    hojin = kinmu = honsya = tdfk = skts = koyo = title = syokusyu = gyousyu = denwa = kyuyo = sigoto = syutoku = baitai = url = ''

    try:
        url = url_df['url'][i]
        tdfk = url_df['（本社住所）都道府県'][i]
        #各求人にアクセス
        resopnse = req.urlopen(url)
        parse_html = BeautifulSoup(resopnse,'html.parser')
        
        try:
            #本社住所の市区町村の情報を取得
            selector = '#jsi-content-wrapper > header > div.contents-hd-wrap > div > nav > ul > li:nth-child(3) > a > span'
            skts = parse_html.select_one(selector).get_text().split()
        except:
            pass
        
        try:
            #求人のタイトルを取得
            title = parse_html.find_all('div',class_='job-detail-caption-c')[0].get_text().split()
        except:
            pass
        
        try:
            #求人の雇用区分、職種、給与情報を取得
            koyo = parse_html.find_all('div',class_='job-detail-tbl-wrap job-detail-tbl-main-wrap')[0].find_all('dd')[0].get_text().split()
            syokusyu = parse_html.find_all('div',class_='job-detail-tbl-wrap job-detail-tbl-main-wrap')[0].find_all('dd')[0].get_text().split()
            kyuyo = parse_html.find_all('div',class_='job-detail-tbl-wrap job-detail-tbl-main-wrap')[0].find_all('dd')[1].get_text().split()
        except:
            pass
        
        for x in range(0,10):
            try:
                #求人の仕事内容、勤務地住所の情報を取得
                judge = parse_html.find_all('div',class_='job-detail-box-tbl')[0].find_all('dt')[x].get_text()
                if judge == '職種':
                    sigoto = parse_html.find_all('div',class_='job-detail-box-tbl')[0].find_all('dd')[x].get_text().split()
                elif judge == '勤務地':
                    kinmu = parse_html.find_all('div',class_='job-detail-box-tbl')[0].find_all('dd')[x].get_text().split()
            except:
                pass
        
        try:
            #求人の採用担当の電話番号を取得
            denwa = parse_html.find_all('p',class_='detail-tel-num')[0].get_text().split()
        except:
            pass
        
        for y in range(0,5):
            for z in range(1,5):
                try:
                    #法人名と業種と本社住所の情報を取得
                    judge = parse_html.find_all('div',class_='job-detail-box-tbl')[z].find_all('dt')[y].get_text()
                    if judge == '社名（店舗名）':
                        hojin = parse_html.find_all('div',class_='job-detail-box-tbl')[z].find_all('dd')[y].get_text().split()
                    elif judge == '会社事業内容':
                        gyousyu = parse_html.find_all('div',class_='job-detail-box-tbl')[z].find_all('dd')[y].get_text().split()
                    elif judge == '会社住所':
                        honsya = parse_html.find_all('div',class_='job-detail-box-tbl')[z].find_all('dd')[y].get_text().split()
                except:
                    pass
            
        syutoku = '{}/{}/{}'.format(year,month,day)
        baitai = '求人サイト名'
        
        new_list.append(str(hojin))
        new_list.append(str(kinmu))
        new_list.append(str(honsya))
        new_list.append(str(tdfk))
        new_list.append(str(skts))
        new_list.append(str(koyo))
        new_list.append(str(title))
        new_list.append(str(syokusyu))
        new_list.append(str(gyousyu))
        new_list.append(str(denwa))
        new_list.append(str(kyuyo))
        new_list.append(str(sigoto))
        new_list.append(str(syutoku))
        new_list.append(str(baitai))
        new_list.append(str(url))
        return new_list
    except:
        pass

#並列処理の実行を１サイクル１,０００件とした際のサイクル数を計算
joblib_num = len(url_df)//1000 + 1

#関数の並列処理の実行
print('スクレイピング開始')

all_list = list()
#求人データを１,０００件取得するごとに進捗のパーセント表示を行う
for n in tqdm(range(0,joblib_num)):
    try:
        resultList = joblib.Parallel(n_jobs=-1,verbose=3)([joblib.delayed(joblib_get_data)(i) for i in range(n*1000,(n+1)*1000)])
        all_list.extend(resultList)
    except:
        pass

#NoneTypeのデータを削除
all_list_filtered = [x for x in all_list if x is not None]

#データフレームに変換
kekka_pre = pd.DataFrame(all_list_filtered,columns=["法人名","勤務地住所","本社住所","（本社住所）都道府県","（本社住所）市区町村","雇用区分","ページタイトル","職種","業種","電話番号","給与情報","仕事内容","取得日","媒体名","ページURL"])

del url_df
del all_list
del all_list_filtered

print('クレンジング開始')

dfs = []
num = len(kekka_pre) // 50000
for i in tqdm(range(0,num+1)):
    if len(kekka_pre) < 50000:
        kekka = kekka_pre
    elif i == num:
        kekka = kekka_pre[50000*i:].reset_index(drop=True)
    else:
        kekka = kekka_pre[50000*i:50000*(i+1)].reset_index(drop=True)
    
    #データ取得時にsplitしたことで生成された余分な文字列を消去
    kekka['法人名'] = kekka['法人名'].str.replace('[', '').str.replace(']', '').str.replace(',', '').str.replace('.', '').str.replace("'", "").str.replace(" ", "").str.replace('　', '')
    kekka['勤務地住所'] = kekka['勤務地住所'].str.replace('[', '').str.replace(']', '').str.replace(',', '').str.replace('.', '').str.replace("'", "").str.replace(" ", "").str.replace('　', '')
    kekka['本社住所'] = kekka['本社住所'].str.replace('[', '').str.replace(']', '').str.replace(',', '').str.replace('.', '').str.replace("'", "").str.replace(" ", "").str.replace('　', '')
    kekka['（本社住所）市区町村'] = kekka['（本社住所）市区町村'].str.replace('[', '').str.replace(']', '').str.replace(',', '').str.replace('.', '').str.replace("'", "").str.replace(" ", "").str.replace('　', '')
    kekka['ページタイトル'] = kekka['ページタイトル'].str.replace('[', '').str.replace(']', '').str.replace(',', '').str.replace('.', '').str.replace("'", "").str.replace(" ", "").str.replace('　', '')
    kekka['業種'] = kekka['業種'].str.replace('[', '').str.replace(']', '').str.replace(',', '').str.replace('.', '').str.replace("'", "").str.replace(" ", "").str.replace('　', '')
    kekka['電話番号'] = kekka['電話番号'].str.replace('[', '').str.replace(']', '').str.replace(',', '').str.replace('.', '').str.replace("'", "").str.replace(" ", "").str.replace('　', '')
    kekka['給与情報'] = kekka['給与情報'].str.replace('[', '').str.replace(']', '').str.replace(',', '').str.replace('.', '').str.replace("'", "").str.replace(" ", "").str.replace('　', '')
    kekka['仕事内容'] = kekka['仕事内容'].str.replace('[', '').str.replace(']', '').str.replace(',', '').str.replace('.', '').str.replace("'", "").str.replace(" ", "").str.replace('　', '')
    
    #株式会社の略称を修正
    kekka['法人名'] = kekka['法人名'].str.replace('\(株\)','株式会社').str.replace('（株）','株式会社')
    
    #雇用区分の略称を修正
    for i in tqdm(range(0,len(kekka))):
        kekka['雇用区分'][i] = str(kekka['雇用区分'][i])
        kekka['職種'][i] = str(kekka['職種'][i])
        a=b=c=d=e=f=g=h=''
        if '[A]' in kekka['雇用区分'][i]:
            kekka['職種'][i] = kekka['職種'][i].replace('[A]','')
            a='アルバイト,'
        if '[P]' in kekka['雇用区分'][i]:
            kekka['職種'][i] = kekka['職種'][i].replace('[P]','')
            b='パート,'
        if '[社]' in kekka['雇用区分'][i]:
            kekka['職種'][i] = kekka['職種'][i].replace('[社]','')
            c='正社員,'
        if '[契]' in kekka['雇用区分'][i]:
            kekka['職種'][i] = kekka['職種'][i].replace('[契]','')
            d='契約社員,'
        if '[派]' in kekka['雇用区分'][i]:
            kekka['職種'][i] = kekka['職種'][i].replace('[派]','')
            e='派遣社員,'
        if '[紹]' in kekka['雇用区分'][i]:
            kekka['職種'][i] = kekka['職種'][i].replace('[紹]','')
            f='有料職業紹介,'
        if '[委]' in kekka['雇用区分'][i]:
            kekka['職種'][i] = kekka['職種'][i].replace('[委]','')
            g='業務委託契約,'
        if '[代]' in kekka['雇用区分'][i]:
            kekka['職種'][i] = kekka['職種'][i].replace('[代]','')
            h='代理店・フランチャイズ,'
        kekka['雇用区分'][i] = a+b+c+d+e+f+g+h
        try:
            n = kekka['雇用区分'][i].rfind(',')
            kekka['雇用区分'][i] = kekka['雇用区分'][i][:n]
        except:
            pass
    kekka['職種'] = kekka['職種'].str.replace('[', '').str.replace(']', '').str.replace(',', '').str.replace('.', '').str.replace("'", "").str.replace(" ", "").str.replace('　', '')
    
    #正規表現で文字列から電話番号を抽出
    kekka = kekka.assign(電話番号='')
    kekka['電話番号'] = kekka['電話番号'].str.extract('([0-9]{2,5}-[0-9]{1,4}-[0-9]{3,4})')
    
    #正規表現で文字列から給与区分を抽出
    kekka = kekka.assign(給与区分="")
    kekka['給与区分'] = kekka['給与情報'].str.extract('([月時日年][給収額俸]|出来高|歩合)')
    kekka['給与区分'] = kekka['給与区分'].str.replace('収', '給').str.replace('額', '給').str.replace('俸', '給').str.replace('出来高', '歩合').str.replace('歩合', '出来高制・歩合制')
    kekka['給与区分'] = kekka['給与区分'].str.replace('年給', '年収')
    kekka['給与情報'] = kekka['給与情報'].str.replace(',', '').str.replace('円', '')
    
    #正規表現で文字列から給与区分に応じた給与下限を抽出
    kekka = kekka.assign(給与下限="")
    pattern_g = '\D([0-9]{6}?|[0-9]{2}?万)～??'
    pattern_n = '\D([0-9]{5}?|[6-9][0-9]{3}?|[0-9]?万)～??'
    pattern_j = '\D([1-5][0-9]{3}?|[7-9][0-9]{2}?)～??'
    pattern_y = '\D([2-9][0-9]{6}?|[0-9]{8}?|[2-9][0-9]{2}?万|[0-9]{4}?万)～??'

    for i in tqdm(range(0, len(kekka))):
        if kekka['給与区分'][i] == '月給':
            try:
                kekka['給与下限'][i] = re.search(pattern_g, kekka['給与情報'][i]).group()[1:]
            except:
                pass
        elif kekka['給与区分'][i] == '時給':
            try:
                kekka['給与下限'][i] = re.search(pattern_j, kekka['給与情報'][i]).group()[1:]
            except:
                pass
        elif kekka['給与区分'][i] == '日給':
            try:
                kekka['給与下限'][i] = re.search(pattern_n, kekka['給与情報'][i]).group()[1:]
            except:
                pass
        elif kekka['給与区分'][i] == '年収':
            try:
                kekka['給与下限'][i] = re.search(pattern_y, kekka['給与情報'][i]).group()[1:]
            except:
                pass
    
    #正規表現で文字列から給与区分に応じた給与上限を抽出
    kekka = kekka.assign(給与上限="")
    pattern_g = '～+?([0-9]{6}?|[1][0-9]{6}?|[0-9]{2}?万|[1][0-9]{2}?万)'
    pattern_n = '～+?([0-9]{5}?|[6-9][0-9]{3}?[0-9]?万)'
    pattern_j = '～+?([1-5][0-9]{3}?|[7-9][0-9]{2}?)'
    pattern_y = '～+?([2-9][0-9]{6}?|[0-9]{8}?|[2-9][0-9]{2}?万|[0-9]{4}?万)'

    for i in tqdm(range(0, len(kekka))):
        if kekka['給与区分'][i] == '月給':
            try:
                kekka['給与上限'][i] = re.search(pattern_g, kekka['給与情報'][i]).group()[1:]
            except:
                pass
        elif kekka['給与区分'][i] == '時給':
            try:
                kekka['給与上限'][i] = re.search(pattern_j, kekka['給与情報'][i]).group()[1:]
            except:
                pass
        elif kekka['給与区分'][i] == '日給':
            try:
                kekka['給与上限'][i] = re.search(pattern_n, kekka['給与情報'][i]).group()[1:]
            except:
                pass
        elif kekka['給与区分'][i] == '年収':
            try:
                kekka['給与上限'][i] = re.search(pattern_y, kekka['給与情報'][i]).group()[1:]
            except:
                pass
    
    #分析時に給与データを数値として扱えるようにするため万を0000に変換
    kekka['給与下限'] = kekka['給与下限'].str.replace('万', '0000').str.replace('～', '')
    kekka['給与上限'] = kekka['給与上限'].str.replace('万', '0000')
    
    kekka = kekka.reindex(columns=['法人名','勤務地住所','本社住所','（本社住所）都道府県','（本社住所）市区町村','雇用区分','ページタイトル','職種','業種','電話番号','給与区分','給与下限','給与上限','仕事内容','取得日','媒体名','ページURL'])
    dfs.append(kekka)

del kekka_pre
kekka = pd.concat(dfs,ignore_index=True)
del dfs

print('データの格納')

bucket_name = 'aws-scraping'
s3_key = 'monthly_data/求人サイト名_{}年{}月.csv'.format(year,month)

s3 = boto3.resource('s3') 
s3_obj = s3.Object(bucket_name,s3_key)
s3_obj.put(Body=dfrust.to_csv(None).encode('utf_8'))