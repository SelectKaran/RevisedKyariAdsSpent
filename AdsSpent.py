import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from tqdm import tqdm
import io
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
json_data_dict ={
        "type": "service_account",
        "project_id": "driveapi-396610",
        "private_key_id": "c75337f30c0870d8dd585496e02e8dbeb3cf229d",
        "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDqHxHtqSVbKi3X\nr93F4OARfEwerQB54mf8jJ3qO2axfNPPxwut7O72vrIa1FijaTRqvfCcB0aSi/IB\n4cKoL4GegwsBp/Wd+KiHkDoSXv/4N35zN4ZBqNujo4GV2ntgEdmsnIvhAY5NZXbH\nR0FGwItF5jj8PCB6PdI6hSS2zg0NKtySYwkhdHlJxCjHtLu1Rb9N2SGaTudrvLKV\n1r35thZuNbAzuLicuC0DukJT8pxhTbS1ST3RmpA7ug2zQSgkpK5iv0akpnjbDNyx\nz+ytNhWbFyNkzasKTDhd/hMg1RWSnDf98Q4plCbbAZheC/bbEJAi4pt24sWOJUV6\nzxNB8ZSrAgMBAAECggEAGGVGa/pdHyPFBR2ZQV5OWuQV1nh2fTzfUwygA+FOsR3t\nwE/gYq42tFVon60S02xJ/vlt0gRcETct745Dx1yz5/2FrxV+XYiknwOjWXi2uXmm\n3oChp8Pdpy6JeUD77CXQBdGGLdsIpf31o4xEPAgiOxVjSL1HMRWyC1EGY1oTOBTV\nQR1K/LQpQiP1XG/7IclpZZNZZNPjZKc3+WVDj/xcx15MC8swxaNGDx+o5afY0ul7\nApxxLOsCAjnafNV7Es7FnI2Nz4yg5Y1MRRMNey8UHPnmvDL6qa3W3YF4x6B145hp\nWzfogWyrW3a3LZXcRzGKSe+u3kkSyolcOD3AvT9pwQKBgQD5ygM1vnlme0h4v2Ih\nZmVIaY7J71CXj98+3BJhN4oHJFq+FIamxZZDIc8X4MrA9aTqMMZOULMw48v2CwVE\nYKwtUHC/FpystUKFyv1iGbb7UsQG++lM+b6L1cnW4rbc+aAGICmKo0bVPrC06Fco\nFbJGwvNTuF4pA6hDc40UPPibCwKBgQDv8VPJ3XIiWjotv+wN0Jl+iiSrQfsxNiYU\nJdVrq0UvWaJv41HWbfqtBqENiQ7MNbQwqrGnEJXXLmY/5IO3+DvQwdDIqoyvnPRx\nuoAfp2GW0BpKjX2B/X24s9OVKJdl17CfV8CzkhqbV8liwUds2jfOgulKJQKaYZ8h\nYfC5hKXw4QKBgHvnzlHRiyzfyKJE5SuGPIV//xmCQar87hOjXOamgyxpxy10xxpg\n9tmUIsNIearf7w9QZH4in9CHnvwMmW9CuQW9WkAfulYdj8MIX0pTUSY39w8z1JWf\naPq6cOXMDkNs/Akt2Q1xUsii0Urb2agDoyxgtgz4bpTPwJ686eV5HSTjAoGAB/Sr\nf4z9JNBzD2NGs2qQPFbeQmNsrcQK3S4n9mr2X0yMi0MxSnfZEPWgT2+U8wZw1BBE\n1bJCFaFvOH0eNPJhIVnbz1uAUK5WmJLDfskw/iwmQwSP/chm68HiqRZwdqsBKzdg\np1OX2EC/56ta7+wIX6uNiqzRekb0XMn/jlcsnWECgYEA6W1SlsOBIDYkefytsMRn\nETaiSK4Dr+HGwwTRzBmXkl1py9kxyJNQb6X2AKvQCPh9RIydQL9fb6l43kzZrmyF\n2pKHBXgXOhs+nwaYSkw0Lf261/YPbK/st9LTBqfRF6RMP6LJQQHXcgcxVRBnUiJG\nn21J0kxisrMo3dV4Rqzrfvw=\n-----END PRIVATE KEY-----\n",
        "client_email": "driveapi@driveapi-396610.iam.gserviceaccount.com",
        "client_id": "112352709061905236450",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/driveapi%40driveapi-396610.iam.gserviceaccount.com",
        "universe_domain": "googleapis.com"
    }

def initialize_drive_service(json_data_dict):
    creds = service_account.Credentials.from_service_account_info(json_data_dict)
    drive_service = build('drive', 'v3', credentials=creds)
    return drive_service

def download_and_parse_file(drive_service, file_id, file_name):
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    pbar = None

    while not done:
        status, done = downloader.next_chunk()
        if pbar is None:
            pbar = tqdm(total=100, desc="Progress", unit="%",
                        bar_format="{desc} {percentage:3.0f}%|{bar:20}|")
        if status:
            pbar.update(int(status.progress() * 100))

    pbar.close()
    fh.seek(0)

    try:
        if file_name.lower().endswith('.csv'):
            df = pd.read_csv(fh)
        elif file_name.lower().endswith('.xlsx'):
            df = pd.read_excel(fh)
        else:
            print(f"File {file_name} has an unsupported format. Skipping...")
            return None
    except pd.errors.ParserError:
        print(f"File {file_name} cannot be read. Skipping...")
        return None

    return df

def drive_libV2(folder_id):
    drive_service = initialize_drive_service(json_data_dict)
    results = drive_service.files().list(q=f"'{folder_id}' in parents",
                                         fields="nextPageToken, files(id, name)").execute()
    items = results.get('files', [])

    if not items:
        print('No files found. Kindly Shared Editor Request To - driveapi@driveapi-396610.iam.gserviceaccount.com')
        return None

    print('Files:')
    final_df = pd.DataFrame()

    for item in items:
        file_name = item['name']
        file_id = item['id']
        df = download_and_parse_file(drive_service, file_id, file_name)

        if df is not None:
            final_df = pd.concat([final_df, df])

    return final_df
import pandas as pd
import numpy as np
import os
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
raw_sp_df=drive_libV2("1P_ReRDlRS1nr0eR15D8LJQuhjOAiBTlI")
raw_sd_df=drive_libV2("1m4E2do3pDWeC9BBGL0sF-fyElL_rJshA")
columns_list=["Date","Advertised ASIN","Campaign Name","Impressions","Clicks","Spend","14 Day Total Sales (₹)","14 Day Total Units (#)"]
final_sp_df=raw_sp_df[columns_list].copy()
final_sp_df['Category']="SP"
final_sd_df=raw_sd_df[columns_list].copy()
final_sd_df['Category']="SP"
concat_df=pd.concat([final_sp_df,final_sd_df])
concat_df.rename({"Advertised ASIN":"ASIN","14 Day Total Sales (₹)":"Ads Sales","14 Day Total Units (#)":"Units"},axis=1,inplace=True)
concat_df['Month']=concat_df['Date'].dt.strftime('%B')
concat_df['Impressions']=concat_df['Impressions'].astype(int)
concat_df['Units']=concat_df['Units'].astype(int)
concat_df['Clicks']=concat_df['Clicks'].astype(int)
concat_grp_df=concat_df.groupby(['Date',"Campaign Name" ,'ASIN', 'Month', 'Category'])[['Impressions', 'Clicks', 'Spend', 'Ads Sales', 'Units']].sum(numeric_only=True).reset_index()
def find_brand(text):
  if "KYARI.CO" in text.upper():
    return "KYARI"
  elif "ROBOTBANAO.COM" in text.upper():
    return "ROBOTBANAO"
  elif "ROBOT BANAO" in text.upper():
    return "ROBOTBANAO"
  elif "YOUR BOT" in text.upper():
    return "YOURBOT"
  elif "NAN" in text.upper():
    return "GENERIC"
  elif "GENERIC." in text.upper():
    return "GENERIC"
  else:
    return text
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('./emerald-cab-384306-b8566336d0b0.json', scope)
client = gspread.authorize(creds)
gs = client.open('Channel Linking Database ( Res. Saurabh ) ')
gs.client.timeout =1000
sheet=gs.worksheet('Linking')
all_record=sheet.get_all_records()
link_df=pd.DataFrame(all_record)
sort_link=link_df[['Parent SKU', 'Child SKU', 'ItemName ',"Channel ID","Item  Type","Brand","Brand By Keepa"]].copy()
sort_link["New_Brand"] = sort_link.apply(lambda x: x['Brand'] if pd.isna(x['Brand By Keepa']) or x['Brand By Keepa'] == ''or x['Brand By Keepa'] == '#N/A' else x['Brand By Keepa'], axis=1)
sort_col=sort_link[['Parent SKU', 'Child SKU', 'ItemName ', 'Channel ID', 'Item  Type','New_Brand']].copy()
sort_col['New_Brand']=sort_col['New_Brand'].str.upper()
sort_col['Brand']=sort_col['New_Brand'].apply(find_brand)
concat_dfWithName=concat_grp_df.merge(sort_col,left_on='ASIN',right_on='Channel ID',how='left')
sorted_concat_df=concat_dfWithName[['Date', 'Month',"Campaign Name" ,'ASIN','Parent SKU','Child SKU','ItemName ','Item  Type', "Brand",'Impressions', 'Clicks', \
                   'Spend','Units','Ads Sales','Category']].drop_duplicates(ignore_index=True)
gsSKU= client.open('Select._Master_Data_2023')
SKUsheet=gsSKU.worksheet('sku_parent')
SKU_all_record=SKUsheet.get_all_records()
SKU_df=pd.DataFrame(SKU_all_record)
sorted_cat=SKU_df[["Child SKU","Parent Category","Child Category","Material","Packs","Shape","Colour"]].drop_duplicates(ignore_index=True)
final_kyari_both=sorted_concat_df.merge(sorted_cat,on=['Child SKU'],how='left')
final_kyari_both=final_kyari_both[(final_kyari_both["Impressions"]+final_kyari_both["Clicks"]+final_kyari_both["Spend"]+final_kyari_both["Units"]+final_kyari_both["Ads Sales"])>0].copy()
final_kyari_both["Date"]=final_kyari_both["Date"].dt.date
gskyaricomb = client.open('Kyari Advertisment SP,SD&SB(July-Sep2023)')
sheetKyariComb=gskyaricomb.worksheet('SP&SD')
sheetKyariComb.clear()
sheetKyariComb.timeout =120
set_with_dataframe(sheetKyariComb,final_kyari_both)
raw_sb_df=drive_libV2("1M_tk29UAFlIKfhQJoK8_2aV_HxW3TYZ2")
columns_list=["Date","Campaign Name","Impressions","Clicks","Spend","14 Day Total Sales (₹)","14 Day Total Units (#)"]
final_SB_df=raw_sb_df[columns_list].copy()
final_SB_df.rename({"14 Day Total Sales (₹)":"Ads Sales","14 Day Total Units (#)":"Units"},axis=1,inplace=True)
final_SB_df['Impressions']=final_SB_df['Impressions'].astype(int)
final_SB_df['Ads Sales']=final_SB_df['Ads Sales'].astype(int)
final_SB_df['Clicks']=final_SB_df['Clicks'].astype(int)
final_SB_df['Units']=final_SB_df['Units'].astype(int)
print(final_SB_df.shape)
final_sb_df_grp=final_SB_df.groupby(['Date', 'Campaign Name'])[['Impressions', 'Clicks', 'Spend', 'Ads Sales','Units']].sum().reset_index().copy()
print(final_sb_df_grp.shape)
gs = client.open('KYARI Advertisment Raw Data SP/SB/Video/SD')
sheet1=gs.worksheet('SB CAMP Linking')
all_record1=sheet1.get_all_records()
SB_link_df=pd.DataFrame(all_record1)
merge_sku=final_sb_df_grp.merge(SB_link_df,left_on='Campaign Name',right_on='Campaign',how='left').drop_duplicates(ignore_index=True)
merge_sku['Month']=merge_sku['Date'].dt.strftime('%B')
sorted_merge_sku=merge_sku[['Date','Month', "Campaign Name","Parent SKU",'Child SKU','Impressions', 'Clicks', 'Spend', 'Units','Ads Sales']].copy()
sorted_merge_sku['Category']="SB"
sorted_cat=SKU_df[["Child SKU","Parent Category","Child Category","Material","Packs","Shape","Colour"]].drop_duplicates(ignore_index=True)
final_sb_kyari=sorted_merge_sku.merge(sorted_cat,on=['Child SKU'],how='left')
final_sb_kyari=final_sb_kyari[(final_sb_kyari["Impressions"]+final_sb_kyari["Clicks"]+final_sb_kyari["Spend"]+final_sb_kyari["Units"]+final_sb_kyari["Ads Sales"])>0].copy()
final_sb_kyari["Date"]=final_sb_kyari["Date"].dt.date