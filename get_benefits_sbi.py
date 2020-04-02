from time import sleep
import pandas as pd
import re
from datetime import datetime


re_stock_price = re.compile(r'現在値 ([\d,.]+) ')
re_num = re.compile(r'\d+')

def fetch(code):
    url = f'https://site1.sbisec.co.jp/ETGate/?_ControlID=WPLETsiR001Control&_PageID=WPLETsiR001Idtl60&_DataStoreID=DSWPLETsiR001Control&_ActionID=DefaultAID&stock_sec_code_mul={code}'

    df = pd.read_html(url)
    if len(df) < 7:
        return (None, None, None)
    name = df[1][0][0].split('\xa0')[0]
    match = re_stock_price.findall(df[4][0][1])
    if match:
        stock_price = float(match[0].replace(',', ''))
    else:
        stock_price = -1

    base_df = df[6].set_index(0)
    url = base_df.loc['URL', 1]

    listed_company_df = pd.DataFrame(
        ((code, name, url, stock_price),),
        columns=('id', 'name', 'benefit_url', 'stock_price')
    )

    record_dates = base_df.loc['権利確定月', 1].split('\xa0')
    record_date_df = pd.DataFrame(
        ([(code, x) for x in record_dates]),
        columns=('listed_company_id', 'record_date')
    )

    detail_df = df[7]
    detail_df.columns = ('name1', 'name2', 'shares', 'memo')
    content = detail_df.apply(lambda x: x.name1 if x.name1 == x.name2 else x.name1 + ' ' + x.name2, axis=1)
    shares_df = detail_df['shares'].apply(lambda x: 0 if x == '全株主' else int(''.join(re_num.findall(x))))

    shares_list = list()
    for idx, row in shares_df.items():
        min_required_shares = row
        if min_required_shares == 0:
            max_required_shares = -1
        elif shares_df.size - 1 <= idx:
            max_required_shares = -1
        else:
            max_required_shares = shares_df[idx + 1]
            if min_required_shares > max_required_shares:
                max_required_shares = -1
        shares_list.append((min_required_shares, max_required_shares))

    benefit_df = pd.concat([
        content.apply(lambda x: code),
        content,
        pd.DataFrame(shares_list),
        detail_df['memo']
    ], axis=1)
    benefit_df.columns = ('listed_company_id', 'content', 'min_required_share', 'max_required_share', 'memo')

    return (listed_company_df, record_date_df, benefit_df)

def get_codes():
    url = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
    df = pd.read_excel(url)
    includes = ['市場第一部（内国株）', '市場第二部（内国株）', 'マザーズ（内国株）', 'JASDAQ(グロース・内国株）', 'JASDAQ(スタンダード・内国株）']
    return df[df['市場・商品区分'].isin(includes)]['コード'].values

if __name__ == "__main__":
    listed_company_df_list = list()
    benefit_df_list = list()
    record_date_df_list = list()

    for code in get_codes():
        sleep(0.2)
        print(code)
        listed_company_df, record_date_df, benefit_df = fetch(code)
        if benefit_df is None:
            continue
        listed_company_df_list.append(listed_company_df)
        record_date_df_list.append(record_date_df)
        benefit_df_list.append(benefit_df)

    pd.concat(listed_company_df_list).to_csv('./data/raw/listed_company.csv', index=False)
    pd.concat(record_date_df_list).to_csv('./data/raw/record_date.csv', index=False)
    pd.concat(benefit_df_list).to_csv('./data/raw/benefit.csv', index=False)