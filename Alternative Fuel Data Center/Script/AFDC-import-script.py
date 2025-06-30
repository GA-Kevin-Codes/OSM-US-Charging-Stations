import requests
import pandas as pd
import datetime
import io
import re
import json
import sys

# Constants
API_URL = "https://developer.nrel.gov/api/alt-fuel-stations/v1/ev-charging-units.csv"
API_KEY = "Ja5Bj5oxwyXIhCnfpZBDpkaon2Q4bx4FsghczvDm"
CONNECTOR_CONFIG = [
    ("EV CCS Connector Count", "socket:type1_combo", "EV CCS Power Output (kW)"),
    ("EV CHAdeMO Connector Count", "socket:chademo", "EV CHAdeMO Power Output (kW)"),
    ("EV J3400 Connector Count", "socket:nacs", "EV J3400 Power Output (kW)"),
]
NETWORK_MAP = {
    "eVgo Network": "EVgo",
    "SHELL_RECHARGE": "Shell Recharge",
    "ChargePoint Network": "ChargePoint",
    "Blink Network": "Blink",
    "RED_E": "Red E",
    "CHARGELAB": "ChargeLab",
}
ADDRESS_EXPANSIONS = {
    r"\bSt\b": "Street",
    r"\bAve\b": "Avenue",
    r"\bRd\b": "Road",
    r"\bBlvd\b": "Boulevard",
    r"\bDr\b": "Drive",
    r"\bLn\b": "Lane",
    r"\bHwy\b": "Highway",
    r"\bPkwy\b": "Parkway",
    r"\bPl\b": "Place",
    r"\bCt\b": "Court",
    r"\bFwy\b": "Freeway",
    r"\bSq\b": "Square",
    r"\bCirc\b": "Circle",
    r"\bRt\b": "Route",
    r"\bTpke\b": "Turnpike",
    r"\bN\b": "North",
    r"\bS\b": "South",
    r"\bE\b": "East",
    r"\bW\b": "West",
    r"\bNE\b": "Northeast",
    r"\bSE\b": "Southeast",
    r"\bSW\b": "Southwest",
    r"\bNW\b": "Northwest",
}

def get_week_range():
    """
    Compute the date range for last week (Sunday to Saturday). Must run on Sunday.
    """
    today = datetime.date.today()
    if today.weekday() != 6:
        raise ValueError("Script must be run on Sunday.")
    start = today - datetime.timedelta(days=7)
    end = today - datetime.timedelta(days=1)
    return start, end


def fetch_data():
    """
    Fetch EV charging data as CSV from NREL API and load into DataFrame.
    """
    params = {
        "access": "public",
        "api_key": API_KEY,
        "download": "true",
        "fuel_type": "ELEC",
        "ev_charging_level": "dc_fast",
        "status": "E",
        "country": "US",
        "utf8_bom": "true",
        "limit": "all",
    }
    response = requests.get(API_URL, params=params)
    response.raise_for_status()
    return pd.read_csv(io.StringIO(response.text), low_memory=False)


def filter_by_date(df, column, start, end):
    """
    Filter DataFrame rows where `column` date is between start and end.
    """
    df[column] = pd.to_datetime(df[column], errors='coerce').dt.date
    return df[df[column].between(start, end)].copy()


def expand_address(raw_addr):
    """
    Split house number and street, expand abbreviations, preserve 'U.S.'.
    """
    addr = str(raw_addr).split(',')[0].strip()
    match = re.match(r'^(?P<num>\d+)\s+(?P<street>.+)$', addr)
    if not match:
        return "", addr
    num, street = match.group('num'), match.group('street')
    # Preserve U.S.
    placeholder = '__US__'
    street = street.replace('U.S.', placeholder)
    # Expand common abbreviations
    for pattern, repl in ADDRESS_EXPANSIONS.items():
        street = re.sub(pattern, repl, street)
    street = street.replace(placeholder, 'U.S.')
    return num, street


def format_phone(raw_phone):
    """
    Normalize phone numbers to +1-XXX-XXX-XXXX format.
    """
    digits = re.sub(r'\D', '', str(raw_phone))
    if len(digits) == 11 and digits.startswith('1'):
        digits = digits[1:]
    if len(digits) == 10:
        return f"+1-{digits[:3]}-{digits[3:6]}-{digits[6:]}"
    return str(raw_phone)


def correct_hours(raw_hours):
    """
    Interactive correction for opening hours, default to 24/7 when blank.
    """
    text = str(raw_hours).strip()
    if text in ('', 'nan', '24 hours daily'):
        return '24/7'
    print(f"Please correct opening hours for [{text}]: ", end='')
    return sys.stdin.readline().strip()


def process_connectors(df):
    """
    Generate connector count and output tags based on configuration.
    """
    for count_col, tag, power_col in CONNECTOR_CONFIG:
        counts = df[count_col]
        df[tag] = counts.where(counts >= 1, '')
        df[f"{tag}:output"] = df[power_col].where(counts >= 1, '').apply(
            lambda x: str(int(float(x))) if pd.notnull(x) and str(x).strip() not in ('', 'nan') else ''
        )
    return df


def main():
    # 1. Date range for last week
    start_week, end_week = get_week_range()

    # 2. Fetch and filter data
    df = fetch_data()
    df_week = filter_by_date(df, 'Open Date', start_week, end_week)

    # 3. Parse and expand addresses
    addresses = df_week['Street Address'].apply(expand_address)
    df_week[['addr:housenumber', 'addr:street']] = pd.DataFrame(addresses.tolist(), index=df_week.index)
    df_week['addr:city'] = df_week['City']
    df_week['addr:state'] = df_week['State']
    df_week['addr:postcode'] = df_week['ZIP'].astype(str).str.zfill(5)
    df_week['addr:country'] = df_week['Country']

    # 4. Format phone and opening hours
    df_week['network:phone'] = df_week['Station Phone'].apply(format_phone)
    df_week['opening_hours'] = df_week['Access Days Time'].apply(correct_hours)

    # 5. Map network info
    df_week['network'] = df_week['EV Network'].map(NETWORK_MAP).fillna(df_week['EV Network'])
    df_week['network:website'] = df_week['EV Network Web']

    # 6. Round coordinates
    df_week['Latitude'] = df_week['Latitude'].round(6)
    df_week['Longitude'] = df_week['Longitude'].round(6)

    # 7. Prepare IDs and static tags
    df_week['start_date'] = df_week['Open Date'].astype(str)
    df_week['check_date'] = pd.to_datetime(df_week['Date Last Confirmed'], errors='coerce').dt.date.astype(str)
    df_week['ref:afdc'] = df_week['ID'].astype(str)
    df_week['man_made'], df_week['frequency'], df_week['access'] = 'charge_point', 0, 'yes'

    # 8. Connector tags
    df_week = process_connectors(df_week)

    # Columns to include
    tag_cols = [
        'addr:housenumber','addr:street','addr:city','addr:state','addr:postcode',
        'addr:country','network:phone','opening_hours','network','network:website',
        'Latitude','Longitude','check_date','ref:afdc','start_date',
        'socket:type1_combo','socket:type1_combo:output',
        'socket:chademo','socket:chademo:output',
        'socket:nacs','socket:nacs:output','man_made','frequency','access'
    ]

    # 9. Separate individual points to filter
    points = df_week[tag_cols].fillna('')
    group_keys = [
        'addr:housenumber','addr:street','addr:city','addr:state','addr:postcode',
        'addr:country','network:phone','opening_hours','network','network:website'
    ]
    uniq = df_week.groupby(group_keys)[['Latitude','Longitude']].nunique().reset_index()
    single = uniq[(uniq['Latitude']==1)&(uniq['Longitude']==1)][group_keys]
    drops = set(tuple(x) for x in single.values)
    points_filtered = points[~points[group_keys].apply(tuple, axis=1).isin(drops)]

    # 10. Aggregate grouped stations
    stations = []
    for keys, grp in df_week.groupby(group_keys):
        rec = dict(zip(group_keys, keys))
        rec.update({
            'amenity': 'charging_station',
            'Latitude': round(grp['Latitude'].mean(), 6),
            'Longitude': round(grp['Longitude'].mean(), 6),
            'ref:afdc': ';'.join(sorted(grp['ref:afdc'].unique())),
            'start_date': ';'.join(sorted(grp['start_date'].unique())),
            'check_date': ';'.join(sorted(grp['check_date'].unique())),
        })
        for _, tag, _ in CONNECTOR_CONFIG:
            counts = grp.loc[grp[tag] != '', tag].astype(int)
            rec[tag] = counts.sum() if not counts.empty else ''
            outs = grp.loc[grp[f"{tag}:output"] != '', f"{tag}:output"].unique()
            rec[f"{tag}:output"] = ';'.join(sorted(str(x) for x in outs)) if len(outs) else ''
        stations.append(rec)
    stations_df = pd.DataFrame(stations, columns=tag_cols+['amenity']).fillna('')

    # 11. Combine and export
    final_df = pd.concat([points_filtered, stations_df], sort=False).fillna('')
    final_df.to_csv('ev_charging_weekly_snapshot.csv', index=False)

    # 12. Export GeoJSON
    features = []
    for _, row in final_df.iterrows():
        props = {k: v for k, v in row.to_dict().items() if k not in ('Latitude','Longitude') and v not in ('', None)}
        features.append({
            'type': 'Feature',
            'geometry': {'type': 'Point', 'coordinates': [row['Longitude'], row['Latitude']]},
            'properties': props
        })
    with open('ev_charging_weekly_snapshot.geojson', 'w') as f:
        json.dump({'type': 'FeatureCollection', 'features': features}, f)

    print(f"Exports saved for dates {start_week} to {end_week}: CSV and GeoJSON.")


if __name__ == '__main__':
    main()
