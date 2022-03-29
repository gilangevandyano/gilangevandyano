import uuid

from datetime import timedelta, datetime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, Column, String, Integer, Float, DateTime, Date, Time, Text, BigInteger, Boolean


weather_description_id = {
    "0": "Cerah",
    "1": "Cerah Berawan",
    "2": "Cerah Berawan",
    "3": "Berawan",
    "4": "Berawan Tebal",
    "5": "Udara Kabut",
    "10": "Asap",
    "45": "Kabut",
    "60": "Hujan Ringan",
    "61": "Hujan Sedang",
    "63": "Hujan Lebat",
    "80": "Hujan Lokal",
    "95": "Hujan Petir",
    "97": "Hujan Petir"
}


weather_description_en = {
    "0": "Clear Skies",
    "1": "Partly Cloudy",
    "2": "Partly Cloudy",
    "3": "Mostly Cloudy",
    "4": "Overcast",
    "5": "Haze",
    "10": "Smoke",
    "45": "Fog",
    "60": "Light Rain",
    "61": "Rain",
    "63": "Heavy Rain",
    "80": "Isolated Shower",
    "95": "Severe Thunderstorm",
    "97": "Severe Thunderstorm"
}


wd_card_description = {
    "N": "North",
    "NNE": "North-Northeast",
    "NE": "Northeast",
    "ENE": "East-Northeast",
    "E": "East",
    "ESE": "East-Southeast",
    "SE": "Southeast",
    "SSE": "South-Southeast",
    "S": "South",
    "SSW": "South-Southwest",
    "SW": "Southwest",
    "WSW": "West-Southwest",
    "W": "West",
    "WNW": "West-Northwest",
    "NW": "Northwest",
    "NNW": "North-Northwest",
    "VARIABLE": "berubah-ubah"
}


class Connection(object):

    def __init__(self, db_connection):
        engine = create_engine(db_connection)
        self.engine = engine

    def get_session(self):
        Session = sessionmaker(bind=self.engine)

        return Session()

    def get_engine(self):
        return self.engine


Base = declarative_base()


def init_db(db_connection):
    engine = create_engine(db_connection)
    Base.metadata.create_all(bind=engine)


class Weather(Base):
    __tablename__ = 'weather'

    id = Column(String, primary_key=True)
    area_id = Column(Integer)
    lat = Column(Float)
    lng = Column(Float)
    area_type = Column(String)
    region = Column(String)
    area_level = Column(String)
    description = Column(String)
    domain = Column(String)
    weather_date = Column(DateTime)
    humidity = Column(Integer)
    temperature_c = Column(Float)
    temperature_f = Column(Float)
    weather = Column(Integer)
    weather_description_id = Column(String)
    weather_description_en = Column(String)
    wind_direction_deg = Column(Float)
    wind_direction_card = Column(String)
    wind_direction_sexa = Column(Integer)
    wind_direction_card_description = Column(String)
    wind_speed_kt = Column(Integer)
    wind_speed_mph = Column(Float)
    wind_speed_kph = Column(Float)
    wind_speed_ms = Column(Float)
    fetch_date = Column(DateTime)


    def __init__(self, idx, area_id, lat, lng, area_type, region, area_level, description, domain, weather_date):
        self.id = idx
        self.area_id = area_id
        self.lat = lat
        self.lng = lng
        self.area_type = area_type
        self.region = region
        self.area_level = area_level
        self.description = description
        self.domain = domain
        self.weather_date = weather_date
        self.fetch_date = datetime.now()

    def add_attr(self, idx, elements):
        getattr(self, idx)(elements)

    def hu_hourly(self, elements):
        self.humidity = int(elements.findtext('value'))

    def t_hourly(self, element):
        values = element.findall('value')
        for value in values:
            unit = value.attrib['unit'].lower()
            if unit == 'c':
                self.temperature_c = float(value.text)
            elif unit == 'f':
                self.temperature_f = float(value.text)

    def weather_hourly(self, element):
        weather_code = element.findtext('value')
        self.weather = int(weather_code)
        self.weather_description_id = weather_description_id[weather_code]
        self.weather_description_en = weather_description_en[weather_code]

    def wd_hourly(self, element):
        values = element.findall('value')
        for value in values:
            unit = value.attrib['unit'].lower()
            if unit == 'deg':
                self.wind_direction_deg = float(value.text)
            elif unit == 'card':
                self.wind_direction_card = value.text
                self.wind_direction_card_description = wd_card_description[value.text]
            elif unit == 'sexa':
                self.wind_direction_sexa = int(value.text)

    def ws_hourly(self, element):
        values = element.findall('value')
        for value in values:
            unit = value.attrib['unit'].lower()
            if unit == 'kt':
                self.wind_speed_kt = int(value.text)
            elif unit == 'mph':
                self.wind_speed_mph = float(value.text)
            elif unit == 'kph':
                self.wind_speed_kph = float(value.text)
            elif unit == 'ms':
                self.wind_speed_ms = float(value.text)


class Pemakaman(Base):
    __tablename__ = 'pemakaman'

    no = Column(String, primary_key=True)
    tanggal_permohonan = Column(Date)
    jam_permohonan = Column(Time)
    nama_pemohon = Column(String)
    lokasi_jemput_jenazah = Column(String)
    nama_jenazah = Column(String)
    nik_jenazah = Column(String)
    jenis_kelamin = Column(String)
    usia_jenazah = Column(Integer)
    agama_jenazah = Column(String)
    tanggal_angkut = Column(Date)
    dimakamkan = Column(String)
    diagnosa = Column(String)
    surat_keterangan_rumah_sakit = Column(String)
    petugas_kendaraan = Column(String)
    token = Column(String)

    def __init__(self, no, tanggal_permohonan, jam_permohonan, nama_pemohon, lokasi_jemput_jenazah, nama_jenazah, nik_jenazah, jenis_kelamin,
    usia_jenazah,agama_jenazah, tanggal_angkut, dimakamkan, diagnosa, surat_keterangan_rumah_sakit, petugas_kendaraan, token):

        self.no = no
        self.tanggal_permohonan = tanggal_permohonan
        self.jam_permohonan = jam_permohonan
        self.nama_pemohon = nama_pemohon
        self.lokasi_jemput_jenazah = lokasi_jemput_jenazah
        self.nama_jenazah = nama_jenazah
        self.nik_jenazah = nik_jenazah
        self.jenis_kelamin = jenis_kelamin
        self.usia_jenazah = int(usia_jenazah)
        self.agama_jenazah = agama_jenazah
        self.tanggal_angkut = tanggal_angkut
        self.dimakamkan = dimakamkan
        self.diagnosa = diagnosa
        self.surat_keterangan_rumah_sakit = surat_keterangan_rumah_sakit
        self.petugas_kendaraan = petugas_kendaraan
        self.token = token


class PemakamanAll(Base):
    __tablename__ = 'pemakaman_all_tpu_dki'

    no = Column(Integer)
    tanggal_meninggal = Column(DateTime)
    nik = Column(String)
    nama_jenazah = Column(String)
    tanggal_lahir = Column(DateTime)
    j_tanggal_meninggal = Column(String)
    usia_jenazah = Column(Integer)
    tanggal_kubur = Column(DateTime)
    nama_tpu = Column(String)
    token = Column(String, primary_key=True)

    def __init__(self, no, tanggal_meninggal,nik, nama_jenazah, tanggal_lahir, j_tanggal_meninggal, usia_jenazah, tanggal_kubur, nama_tpu, token):
        self.no = no
        self.tanggal_meninggal = tanggal_meninggal
        self.nik = nik
        self.nama_jenazah = nama_jenazah
        self.tanggal_lahir = tanggal_lahir
        self.j_tanggal_meninggal = j_tanggal_meninggal
        self.usia_jenazah = int(usia_jenazah)
        self.tanggal_kubur = tanggal_kubur
        self.nama_tpu = nama_tpu
        self.token = token

class WazeAlert(Base):
    __tablename__ = 'waze_alerts'

    uuid = Column(String, primary_key=True)
    country = Column(String)
    city = Column(String)
    report_rating = Column(Integer)
    confidence = Column(Integer)
    reliability = Column(Integer)
    type = Column(String)
    road_type = Column(Integer)
    magvar = Column(Integer)
    subtype = Column(String)
    street = Column(Text)
    location_x = Column(Float)
    location_y = Column(Float)
    pub_millis = Column(BigInteger)
    pub_date = Column(DateTime)

    def __init__(self, alert):
        self.uuid = alert.get('uuid')
        self.country = alert.get('country')
        self.city = alert.get('city')
        self.report_rating = alert.get('reportRating')
        self.confidence = alert.get('confidence')
        self.reliability = alert.get('reliability')
        self.type = alert.get('reliability')
        self.road_type = alert.get('roadType')
        self.magvar = alert.get('magvar')
        self.subtype = alert.get('subtype')
        self.street = alert.get('street')
        location = alert.get('location')
        if location:
            self.location_x = location.get('x')
            self.location_y = location.get('y')
        self.pub_millis = alert.get('pubMillis')
        if 'pubMillis' in alert:
            ms = alert.get('pubMillis')
            pub_date = datetime.utcfromtimestamp(ms // 1000).replace(microsecond=ms % 1000 * 1000)
            pub_date = pub_date + timedelta(hours=7)
            self.pub_date = pub_date.strftime("%Y-%m-%d %H:%M:%S")


class WazeIrregularity(Base):
    __tablename__ = 'waze_irregularities'

    jsc_id = Column(String, primary_key=True)
    id = Column(BigInteger)
    country = Column(String)
    city = Column(String)
    n_thumbs_up = Column(Integer)
    trend = Column(Integer)
    line_x = Column(Float)
    line_y = Column(Float)
    type = Column(String)
    speed = Column(Float)
    seconds = Column(Integer)
    street = Column(Text)
    jam_level = Column(Integer)
    n_comments = Column(Integer)
    highway = Column(Boolean)
    delay_seconds = Column(Integer)
    severity = Column(Integer)
    drivers_count = Column(Integer)
    alerts_count = Column(Integer)
    length = Column(Integer)
    n_images = Column(Integer)
    regular_speed = Column(Float)
    update_date_millis = Column(BigInteger)
    update_date = Column(DateTime)
    detection_date_millis = Column(BigInteger)
    detection_date = Column(DateTime)

    def __init__(self, irregularity, linex, liney):
        self.jsc_id = uuid.uuid4()
        self.id = irregularity.get('id')
        self.country = irregularity.get('country')
        self.city = irregularity.get('city', None)
        self.n_thumbs_up = irregularity.get('nThumbsUp')
        self.trend = irregularity.get('trend')
        self.line_x = linex
        self.line_y = liney
        self.type = irregularity.get('type')
        self.speed = irregularity.get('speed')
        self.seconds = irregularity.get('seconds')
        self.street = irregularity.get('street')
        self.jam_level = irregularity.get('jamLevel')
        self.n_comments = irregularity.get('nComments')
        self.highway = irregularity.get('highway')
        self.delay_seconds = irregularity.get('delaySeconds')
        self.severity = irregularity.get('severity')
        self.drivers_count = irregularity.get('driversCount')
        self.alerts_count = irregularity.get('alertsCount')
        self.length = irregularity.get('length')
        self.n_images = irregularity.get('nImages')
        self.regular_speed = irregularity.get('regularSpeed')
        self.update_date_millis = irregularity.get('updateDateMillis')
        if 'updateDateMillis' in irregularity:
            ms = irregularity.get('updateDateMillis')
            pub_date = datetime.utcfromtimestamp(ms // 1000).replace(microsecond=ms % 1000 * 1000)
            pub_date = pub_date + timedelta(hours=7)
            self.update_date = pub_date.strftime("%Y-%m-%d %H:%M:%S")
        self.detection_date_millis = irregularity.get('detectionDateMillis')
        if 'detectionDateMillis' in irregularity:
            ms = irregularity.get('detectionDateMillis')
            pub_date = datetime.utcfromtimestamp(ms // 1000).replace(microsecond=ms % 1000 * 1000)
            pub_date = pub_date + timedelta(hours=7)
            self.detection_date = pub_date.strftime("%Y-%m-%d %H:%M:%S")


class WazeJams(Base):
    __tablename__ = 'waze_jams'

    id = Column(String, primary_key=True)
    uuid = Column(String)
    country = Column(String)
    city = Column(String)
    level = Column(Integer)
    line_x = Column(Float)
    line_y = Column(Float)
    speed_kmh = Column(Float)
    length = Column(Integer)
    type = Column(String)
    speed = Column(Float)
    road_type = Column(Integer)
    delay = Column(Integer)
    street = Column(Text)
    pub_millis = Column(BigInteger)
    pub_date = Column(DateTime)

    def __init__(self, jam, line_x, line_y):
        self.id = uuid.uuid4()
        self.uuid = jam.get('uuid')
        self.country = jam.get('country', None)
        self.city = jam.get('city', None)
        self.level = jam.get('level', None)
        self.line_x = line_x
        self.line_y = line_y
        self.speed_kmh = jam.get('speedKMH', None)
        self.length = jam.get('length', None)
        self.type = jam.get('type', None)
        self.speed = jam.get('speed', None)
        self.road_type = jam.get('roadType', None)
        self.delay = jam.get('delay', None)
        self.street = jam.get('street', None)
        self.pub_millis = jam.get('pubMillis')
        if 'pubMillis' in jam:
            ms = jam.get('pubMillis')
            pub_date = datetime.utcfromtimestamp(ms//1000).replace(microsecond=ms%1000*1000)
            pub_date = pub_date + timedelta(hours=7)
            self.pub_date = pub_date.strftime("%Y-%m-%d %H:%M:%S")

class Gowes(Base):
    __tablename__ = 'gowes'

    jsc_id = Column(String, primary_key=True)
    id = Column(String)
    qr_code = Column(Float)
    type_unit = Column(String)
    group_id = Column(Integer)
    group_name = Column(String)
    rent_start = Column(DateTime)
    rent_end = Column(DateTime)
    rent_minutes = Column(Integer)
    user_start_latitude = Column(Float)
    user_start_longitude = Column(Float)
    user_end_latitude = Column(Float)
    user_end_longitude = Column(Float)
    device_start_latitude = Column(Float)
    device_start_longitude = Column(Float)
    device_end_latitude = Column(Float)
    device_end_longitude = Column(Float)
    speed = Column(Float)
    datetime = Column(DateTime)
    latitude = Column(String)
    longitude = Column(String)
    
    def __init__(self, gowes, datetime, latitude, longitude):
            self.jsc_id = uuid.uuid4()
            self.id =  gowes.get('id') 
            self.qr_code = gowes.get('qr_code', None) 
            self.type_unit = gowes.get('type_unit', None) 
            self.group_id = gowes.get('group_id', None) 
            self.group_name = gowes.get('group_name', None) 
            self.rent_start = gowes.get('rent_start', None) 
            self.rent_end = gowes.get('rent_end', None) 
            self.rent_minutes = gowes.get('rent_minutes', None) 
            self.user_start_latitude = gowes.get('user_start_latitude', None) 
            self.user_start_longitude = gowes.get('user_start_longitude', None) 
            self.device_start_latitude = gowes.get('device_start_latitude', None) 
            self.device_start_longitude = gowes.get('device_start_longitude', None) 
            self.speed = gowes.get('speed', None) 
            self.datetime = datetime
            self.latitude = latitude 
            self.longitude = longitude 

class Ispu(Base):
    __tablename__ = 'ispu'

    tanggal = Column(DateTime)
    id_stasiun = Column(String)
    nama_lokasi = Column(String)
    pm10 = Column(Float)
    so2 = Column(Float)
    co = Column(Float)
    o3 = Column(Float)
    no2 = Column(Float)
    pm25 = Column(Float)
    jsc_id = Column(String, primary_key=True)

    def __init__(self, tanggal, id_stasiun,nama_lokasi,pm10,so2,co,o3,no2,pm25,jsc_id ):
        self.tanggal = tanggal
        self.id_stasiun = id_stasiun
        self.nama_lokasi = nama_lokasi
        self.pm10 = pm10
        self.so2 = so2
        self.co = co
        self.o3 = o3
        self.no2 = no2
        self.pm25 = pm25
        self.jsc_id = jsc_id

class IspuKonsentrat(Base):
    __tablename__ = 'ispu_konsentrat'

    tanggal = Column(DateTime)
    id_stasiun = Column(String)
    nama_lokasi = Column(String)
    jenis_parameter = Column(String)
    nama_parameter = Column(String)
    value = Column(Float)
    satuan = Column(Float)
    jsc_id = Column(String, primary_key=True)

    def __init__(self, tanggal, id_stasiun,nama_lokasi, jenis_parameter, nama_parameter, value, satuan, jsc_id ):
        self.tanggal = tanggal
        self.id_stasiun = id_stasiun
        self.nama_lokasi = nama_lokasi
        self.jenis_parameter = jenis_parameter
        self.nama_parameter = nama_parameter
        self.value = value
        self.satuan = satuan
        self.jsc_id = jsc_id

class Ipj(Base):
    __tablename__ = 'ipj'

    id = Column(Integer)
    nama = Column(String)
    harga = Column(Float)
    tanggal = Column(DateTime)
    id_lokasi = Column(Integer)
    lokasi = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    jsc_id = Column(String, primary_key=True)

    def __init__(self, id, nama, harga, tanggal, id_lokasi, lokasi, latitude, longitude, jsc_id ):
        self.id = id
        self.nama =nama
        self.harga = harga
        self.tanggal = tanggal
        self.id_lokasi = id_lokasi
        self.lokasi = lokasi
        self.latitude = latitude
        self.longitude = longitude
        self.jsc_id = jsc_id