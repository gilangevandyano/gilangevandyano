import argparse
import ssl
import xml.etree.ElementTree as eT

import config
from datetime import datetime
from model import Weather, Connection



def main(db_connection, date):
    ssl._create_default_https_context = ssl._create_unverified_context
    doc = eT.parse(urlopen(config.BMKG_API))

    root = doc.getroot()
    forecast = root.find("forecast")
    areas = forecast.findall("area")
    weathers = {}
    for area in areas:
        area_id = area.attrib['id']
        lat = area.attrib['latitude']
        lng = area.attrib['longitude']
        area_type = area.attrib['type']
        region = area.attrib['region']
        area_level = area.attrib['level']
        description = area.attrib['description']
        domain = area.attrib['domain']

        for params in area:
            if 'id' in params.attrib:
                for timeranges in params:
                    idx = params.attrib['id'] + "_" + params.attrib['type']
                    time = timeranges.attrib['datetime']
                    key = area_id + "_" + time
                    weather_date = datetime.strptime(time, '%Y%m%d%H%M')
                    if date is not None:
                        if weather_date.strftime("%Y-%m-%d") == date:
                            if key not in weathers:
                                weathers[key] = Weather(
                                    idx=key,
                                    area_id=area_id,
                                    lat=lat,
                                    lng=lng,
                                    area_type=area_type,
                                    region=region,
                                    area_level=area_level,
                                    description=description,
                                    domain=domain,
                                    weather_date=weather_date
                                )

                            if params.attrib['type'] == 'hourly':
                                weathers[key].add_attr(idx, timeranges)
                    else:
                        if key not in weathers:
                            weathers[key] = Weather(
                                idx=key,
                                area_id=area_id,
                                lat=float(lat),
                                lng=float(lng),
                                area_type=area_type,
                                region=region,
                                area_level=area_level,
                                description=description,
                                domain=domain,
                                weather_date=weather_date
                            )

                        if params.attrib['type'] == 'hourly':
                            weathers[key].add_attr(idx, timeranges)

    connection = Connection(db_connection)
    session = connection.get_session()
    data = [v for k, v in weathers.items()]

    session.bulk_save_objects(data)
    session.commit()
    session.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--connection", required=True, type=str)
    parser.add_argument("--date", type=str)

    args = parser.parse_args()

    main(args.connection, args.date)