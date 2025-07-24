import requests
import pandas as pd
import re
import json
from io import StringIO

# --- Configuration ---
API_URL = 'https://developer.nrel.gov/api/alt-fuel-stations/v1/ev-charging-units.csv'
API_KEY = 'Ja5Bj5oxwyXIhCnfpZBDpkaon2Q4bx4FsghczvDm'

CONNECTORS = {
    'socket:type1': ('EV J1772 Connector Count', 'EV J1772 Power Output (kW)'),
    'socket:type1_combo': ('EV CCS Connector Count', 'EV CCS Power Output (kW)'),
    'socket:chademo': ('EV CHAdeMO Connector Count', 'EV CHAdeMO Power Output (kW)'),
    'socket:nacs': ('EV J3400 Connector Count', 'EV J3400 Power Output (kW)'),
}

NETWORK_MAP = {
    'eVgo Network': 'EVgo', 'SHELL_RECHARGE': 'Shell Recharge', 'ChargePoint Network': 'ChargePoint',
    'IONNA': 'IONNA', 'ABM': 'ABM', 'FCN': 'Francis Energy', 'Blink Network': 'Blink',
    'RED_E': 'Red E', 'CHARGELAB': 'ChargeLab', 'RIVIAN_ADVENTURE': 'Rivian Adventure',
    'RIVIAN_WAYPOINTS': 'Rivian Waypoints', 'ELECTRIC_ERA': 'Electric Era', 'BP_PULSE': 'bp pulse',
    '7CHARGE': '7Charge', 'APPLEGREEN': 'Applegreen', 'CIRCLE_K': 'Circle K Charge',
    'ENVIROSPARK': 'EnviroSpark', 'FORD_CHARGE': 'Blue Oval', 'FPLEV': 'FPL EVolution',
    'KWIK_CHARGE': 'Kwik Charge',
}

ADDRESS_EXP = {
    r"\bSt\b": "Street", r"\bAve\b": "Avenue", r"\bRd\b": "Road", r"\bRD\b": "Road", r"\bBlvd\b": "Boulevard",
    r"\bDr\b": "Drive", r"\bLn\b": "Lane", r"\bHwy\b": "Highway", r"\bPkwy\b": "Parkway",
    r"\bPl\b": "Place", r"\bCt\b": "Court", r"\bFwy\b": "Freeway", r"\bSq\b": "Square",
    r"\bCirc\b": "Circle", r"\bRt\b": "Route", r"\bTpke\b": "Turnpike",
    r"\bN\b": "North", r"\bS\b": "South", r"\bE\b": "East", r"\bW\b": "West",
    r"\bNE\b": "Northeast", r"\bSE\b": "Southeast", r"\bSW\b": "Southwest", r"\bNW\b": "Northwest",
}

STATE_CODES = {
    'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD',
    'MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC',
    'SD','TN','TX','UT','VT','VA','WA','WV','WI','WY'
}

# Helper to title-case street names, remove periods, preserve US and state codes, and lowercase ordinal suffixes
def title_street(s: str) -> str:
    if not s:
        return ''
    s = s.replace('.', '')
    parts = s.split()
    out = []
    for part in parts:
        up = part.upper()
        m = re.match(r'^(?P<num>\d+)(?P<suffix>ST|ND|RD|TH)$', up)
        if m:
            out.append(f"{m.group('num')}{m.group('suffix').lower()}")
            continue
        if up == 'US':
            out.append('US')
        elif up in STATE_CODES:
            out.append(up)
        else:
            out.append(part.title())
    return ' '.join(out)

# Fetch data
def fetch_data() -> pd.DataFrame:
    params = {
        "access": "public", "api_key": API_KEY, "download": "true",
        "fuel_type": "ELEC", "ev_charging_level": "dc_fast", "status": "E",
        "country": "US", "utf8_bom": "true", "limit": "all",
    }
    resp = requests.get(API_URL, params=params)
    resp.raise_for_status()
    return pd.read_csv(StringIO(resp.text), low_memory=False)

# Expand and normalize fields
def expand_address(raw):
    addr = str(raw).split(',')[0].strip()
    match = re.match(r'^(?P<num>\d+)\s+(?P<street>.+)$', addr)
    if not match:
        return "", ""
    num, street = match.group('num'), match.group('street')
    placeholder = '__US__'
    street = street.replace('U.S.', placeholder)
    for pat, rep in ADDRESS_EXP.items():
        street = re.sub(pat, rep, street)
    street = street.replace(placeholder, 'US')
    return num, street


def format_phone(raw):
    txt = str(raw).strip().lower()
    if txt == 'nan':
        return ''
    digits = re.sub(r'\D', '', txt)
    if len(digits) == 11 and digits.startswith('1'):
        digits = digits[1:]
    if len(digits) == 10:
        return f"+1 {digits[:3]}-{digits[3:6]}-{digits[6:]}"
    return ''


def correct_hours(raw):
    t = str(raw).strip().lower()
    return '24/7' if t in ('', 'nan') or '24 hours daily' in t else ''


def process_connectors(df):
    for tag, (cnt_col, pow_col) in CONNECTORS.items():
        count = df[cnt_col]
        df[tag] = count.where(count >= 1, '')
        df[f"{tag}:output"] = (
            df[pow_col]
            .where(count >= 1, '')
            .apply(lambda x: str(int(float(x))) if pd.notnull(x) and str(x).strip() not in ('', 'nan') else '')
        )
    return df


def compute_frequency(row):
    has1 = bool(str(row.get('socket:type1', '')))
    other = any(bool(str(row.get(c, ''))) for c in ['socket:type1_combo','socket:chademo','socket:nacs'])
    if has1 and other:
        return '0;60'
    if has1:
        return '60'
    if other:
        return '0'
    return ''

# Main
def main():
    df = fetch_data()

    # Address
    addr_exp = df['Street Address'].apply(expand_address)
    df[['addr:housenumber','addr:street']] = pd.DataFrame(addr_exp.tolist(), index=df.index)
    df['addr:street'] = df['addr:street'].apply(title_street)
    df['addr:city'] = df['City']
    df['addr:state'] = df['State']
    df['addr:postcode'] = df['ZIP'].astype(str).str.zfill(5)
    df['addr:country'] = df['Country']

    # Phone & hours
    df['brand:phone'] = df['Station Phone'].apply(format_phone)
    df['opening_hours'] = df['Access Days Time'].apply(correct_hours)

    # Network & coords
    df['brand'] = df['EV Network'].map(NETWORK_MAP)
    mask = df['brand'].isna()
    df.loc[mask, 'brand'] = df.loc[mask, 'EV Network'].str.replace('_',' ').str.title()
    df['brand:website'] = df['EV Network Web']
    df['Latitude'] = df['Latitude'].round(6)
    df['Longitude'] = df['Longitude'].round(6)

    # Connectors & tags
    df = process_connectors(df)
    df['ref:afdc'] = df['ID'].astype(str)
    df['start_date'] = df['Open Date'].astype(str)
    df['check_date'] = pd.to_datetime(df['Date Last Confirmed'], errors='coerce').dt.date.astype(str)
    df['access'] = 'yes'
    df['motorcar'] = 'designated'

    # Build columns
    static_cols = [
        'addr:housenumber','addr:street','addr:city','addr:state','addr:postcode','addr:country',
        'brand','brand:website','brand:phone','opening_hours','motorcar','access'
    ]
    coord_cols = ['Latitude','Longitude']
    cols = static_cols + ['frequency'] + coord_cols + ['ref:afdc','start_date','check_date']
    for tag in CONNECTORS:
        cols += [tag, f'{tag}:output']
    cols.append('amenity')

    # Group & aggregate
    df['lat_rnd'], df['lon_rnd'] = df['Latitude'], df['Longitude']
    records = []
    for (brand, lat, lon), grp in df.groupby(['brand','lat_rnd','lon_rnd'], dropna=False):
        rec = {c: grp[c].dropna().iloc[0] if not grp[c].dropna().empty else '' for c in static_cols}
        rec.update({'Latitude': lat, 'Longitude': lon, 'amenity': 'charging_station'})
        rec['ref:afdc'] = ';'.join(sorted(grp['ref:afdc'].unique()))
        rec['start_date'] = ';'.join(sorted(grp['start_date'].unique()))
        rec['check_date'] = ';'.join(sorted(grp['check_date'].unique()))
        for tag in CONNECTORS:
            cnts = grp[tag].astype(str).loc[grp[tag].astype(str)!=''].astype(int)
            rec[tag] = cnts.sum() if not cnts.empty else ''
            outs = grp[f'{tag}:output'].loc[grp[f'{tag}:output']!='']
            rec[f'{tag}:output'] = ';'.join(sorted(set(outs.astype(str)))) if not outs.empty else ''
        rec['frequency'] = compute_frequency(pd.Series(rec))
        records.append(rec)

    final_df = pd.DataFrame(records, columns=cols).fillna('')
    final_df.to_csv('afdc_onetimeimport_datatable.csv', index=False)

    # GeoJSON
    features = []
    for _, row in final_df.iterrows():
        props = {k:v for k,v in row.to_dict().items() if k not in ('Latitude','Longitude') and v not in ('','None')}
        features.append({'type':'Feature','geometry':{'type':'Point','coordinates':[row['Longitude'],row['Latitude']]},'properties':props})
    with open('afdc_onetimeimport_geojson.geojson','w') as f:
        json.dump({'type':'FeatureCollection','features':features}, f)

    print('Exports saved: CSV and GeoJSON.')

if __name__ == '__main__':
    main()
