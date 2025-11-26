import pandas as pd

isin_mapping = {
    "Sunteck Realty": "INE805D01034",
    "OBEROI REALTY": "INE093I01010",
    "DLF": "INE271C01023",
    "Embassy Development Ltd": "INE069I01010",
    "PRESTIGE ESTATE": "INE811K01011",
    "HDIL": "INE191I01012",
    "PHOENIX MILLS": "INE211B01039",
    "GODREJ PROPERTIES Ltd.": "INE484J01027",
    "SOBHA LTD.": "INE671H01015",
    "MAHINDRA LIFESPACES": "INE813A01018",
    "OMAXE": "INE800H01010",
    "KOLTE PATIL LTD.": "INE094I01018",
    "BRIGADE ENTERPRISES": "INE791I01019",
    "UNITECH": "INE694A01020",
    "ANANT RAJ": "INE242C01024",
    "ASHIANA HOUSING": "INE365D01021",
    "EMBASSY": "INE041025011",
    "BROOKFIELD": "INE0FDU25010",
    "MINDSPACE": "INE0CCU25019",
    "Lodha Macrotech": "INE670K01029",
    "RUSTOMJEE KEYSTONE": "INE263M01029",
    "Signature Global": "INE903U01023",
    "Puravankara": "INE323I01011",
    "Suraj Estate": "INE843S01025",
    "Arkade Developers": "INE0QRL01017",
    "Kalpataru Ltd": "INE227J01012",
    "Raymond Realty": "INE1SY401010",
    "Sri Lotus Developers": "INE0V9Q01010",
    "Knowledge Realty Trust": "INE1JAR25012",
    "NBCC (India) Ltd.": "INE095N01031",
    "Adtiya Birla Real estate Ltd": "INE055A01016",
    "Valor Estate Ltd": "INE879I01012",
    "Max Estate Ltd": "INE03EI01018",
    "Welspun Enterprise Ltd": "INE625G01013",
    "Ganesh Housing Ltd.": "INE460C01014",
    "Ahluwalia Contracts (India) Ltd.": "INE758C01029",
    "Hubtown Ltd.": "INE703H01016",
    "TARC Ltd.": "INE0EK901012",
    "Marathon Nextgen Realty Ltd.": "INE182D01020",
    "Hemisphere Properties Ltd.": "INE0AJG01018",
    "Ajmera Realty & Infra India Ltd": "INE298G01027"
}

ISIN_list = list(isin_mapping.values())
def Today_Volume(NSE_bhav_today, BSE_bhav_today=None, master_df=None):

    # Load BSE today bhavcopy
    BSE_bhav_today = pd.read_csv('./BSEDATA/BSE_bhavcopy_TODAY.csv')

    # Filter by ISIN list
    NSE_f = NSE_bhav_today[NSE_bhav_today['ISIN'].isin(ISIN_list)]
    BSE_f = BSE_bhav_today[BSE_bhav_today['ISIN'].isin(ISIN_list)]

    # Compute volume per ISIN
    nse_vol = NSE_f.groupby('ISIN')['TtlTradgVol'].sum()
    bse_vol = BSE_f.groupby('ISIN')['TtlTradgVol'].sum()
    print(f"NSE_VOL",nse_vol)
    print(f"BSE_VOL",bse_vol)

    # Total volume = NSE + BSE
    total_volume = nse_vol.add(bse_vol, fill_value=0)

    # Add to master_df
    master_df['Total_Trading_Volume'] = master_df['ISIN'].map(total_volume).fillna(0)
    print(master_df)

    return master_df

    