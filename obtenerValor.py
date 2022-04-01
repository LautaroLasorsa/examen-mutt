from abc import abstractmethod
import sys
import requests
import datetime
import logging
import psycopg2
from re import sub
import time

logging.basicConfig(level=logging.WARNING)

class obtenedor:
    def cotizacion(fecha,moneda):
        logging.debug("https://api.coingecko.com/api/v3/coins/{}/history?date={}".format(moneda,fecha))
        return requests.get("https://api.coingecko.com/api/v3/coins/{}/history?date={}".format(moneda,fecha))

class rangoFechas:
    def rangoFechas(fechaIni, fechaFin, formato = None):
        fecha = fechaIni
        for d in range((fechaFin-fechaIni).days+1):
            if formato!=None:
                yield (fecha+datetime.timedelta(days=d)).strftime(formato)
            else:
                yield (fecha+datetime.timedelta(days=d))

class administrador:
    @abstractmethod
    def guardar(self,fecha,moneda,cotizacion):
        pass

class administradorArchivo(administrador):
    def guardar(self,fecha,moneda,cotizacion):
        nombreArchivo = moneda+'_'+fecha+".json"
        archivo = open(nombreArchivo,'w')
        archivo.write(str(cotizacion))

class administradorPostgres(administrador):
    
    nombre = "postgres"
    usuario = "postgres"
    password = "mysecretpassword"
    host = "localhost"
    puerto = "5432"
    conexion = None
    cursor = None

    def __init__(self,_nombre = None, _usuario=None, _password=None, _host=None, _puerto = None) -> None:
        super().__init__()
        if _nombre != None:
            self.nombre = _nombre
        if _usuario != None:
            self.usuario = _usuario
        if _password != None:
            self.password = _password
        if _host != None:
            self.host = _host
        if _puerto != None:
            self.puerto = _puerto
        self.conexion = psycopg2.connect(
            dbname = self.nombre, user = self.usuario, 
            password = self.password, host = self.host,
            port = self.puerto)
        self.cursor = self.conexion.cursor()
        self.chequeoInicio()

    def chequeoInicio(self):
        try:
            self.cursor.execute("SET DATESTYLE to dmy;")

            self.cursor.execute(
            "SELECT *  FROM INFORMATION_SCHEMA.TABLES\
            WHERE TABLE_NAME = 'full_data';")
            
            if(len(self.cursor.fetchall())==0):
            
                self.cursor.execute("CREATE TABLE full_data( coin_id char(100), date date ,price money  ,json char(7000) , primary key(coin_id, date));")
                self.conexion.commit()
            
            self.cursor.execute(
                "SELECT *  FROM INFORMATION_SCHEMA.TABLES\
                WHERE TABLE_NAME = 'agregate_data';")
            
            if(len(self.cursor.fetchall())==0):
                self.cursor.execute("CREATE TABLE agregate_data( coin_id char(100)  , year int  , month int  , max_price money, min_price money, primary key(coin_id, year, month) );")
                self.conexion.commit()
            
                
        except BaseException as e:
            logging.warning(e)
            self.conexion.rollback()


    def guardar(self,fecha,moneda,cotizacion):
        
        for fechaIt in cotizacion.keys():
            logging.debug(fechaIt)
            try:
                self.cursor.execute("delete from full_data where coin_id='{moneda}' and date='{fecha}'".format(moneda = moneda, fecha=fechaIt))
                precioUsd = cotizacion[fechaIt]['market_data']['current_price']['usd']
                instruccion = "insert into full_data values('{moneda}','{fecha}',{precio},'-');".format(
                    moneda=moneda,fecha=fechaIt, precio = round(precioUsd,2))
                logging.debug(instruccion)
                self.cursor.execute(instruccion)
                self.conexion.commit()
            
            except BaseException as e:
                logging.error("Data insertion at {} has failled".format(fechaIt))
                logging.error(e)                
                self.conexion.rollback()

            try:
                anno=fechaIt[-4:] 
                mes=fechaIt[3:5]
                instruccion = "select * from agregate_data where coin_id = '{moneda}' and year = {anno} and month = {mes}".format(
                    moneda=moneda, anno=anno, mes=mes
                )

                logging.debug(instruccion)
                self.cursor.execute(instruccion)
                result = self.cursor.fetchall()
                precioUsd = cotizacion[fechaIt]['market_data']['current_price']['usd']
                logging.debug(result)
                if(len(result)==0):
                    instruccion = "insert into agregate_data values('{moneda}',{anno},{mes},{precio},{precio});".format(
                        moneda = moneda, anno=anno, mes=mes, precio = precioUsd
                    )
                    logging.debug(instruccion)
                    self.cursor.execute(instruccion)
                    self.conexion.commit()
                else:
                    maxPrecio = float(sub(r'[^\d.]', '', result[0][3]))
                    minPrecio = float(sub(r'[^\d.]', '', result[0][4]))
                    if precioUsd>maxPrecio:
                        maxPrecio = precioUsd
                    if precioUsd<minPrecio:
                        minPrecio = precioUsd
                    
                    instruccion = "update agregate_data set max_price={maxPrecio}, min_price={minPrecio} where coin_id='{moneda}' and \
                        year={anno} and month={mes}".format(maxPrecio=maxPrecio,minPrecio = minPrecio, moneda=moneda,anno=anno, mes=mes)
                    logging.debug(instruccion)
                    self.cursor.execute(instruccion)
                    self.conexion.commit()
                    
            except BaseException as e:
                logging.error("Agregate data insertion at {} has failled".format(fechaIt))
                logging.error(e)                
                self.conexion.rollback()

class recibirParametros:
    def get():
        if(len(sys.argv)<3):
            logging.critical("Too few params : {params}\n".format(params=sys.argv))
            exit()
        try:
            postgresi = sys.argv.index('postgres')
            admin = administradorPostgres(*sys.argv[postgresi+1:])
            sys.argv = sys.argv[:postgresi]
        except:
            admin = administradorArchivo()

        if(len(sys.argv)!=3 and len(sys.argv)!=4):
            logging.critical("You have to pass 2 o 3 arguments\n")
            exit()

        if (len(sys.argv)==3):
            try:
                fechaIni = datetime.datetime.strptime(sys.argv[1], '%Y-%m-%d')
                fechaFin = fechaIni
            except:
                logging.critical("The first argument has to be a date\n")
                exit()
            moneda = sys.argv[2]
        else:
            try:
                fechaIni = datetime.datetime.strptime(sys.argv[1], '%Y-%m-%d')
                fechaFin = datetime.datetime.strptime(sys.argv[2], '%Y-%m-%d')
                if fechaIni>fechaFin:
                    logging.warning("The first date is after the second one. They were swapped\n")
                    fechaIni,fechaFin = fechaFin,fechaIni
            except:
                logging.critical(sys.argv)
                logging.critical("The first and second arguments have to be a date\n")
                exit()
            moneda = sys.argv[3]
            
        return list(rangoFechas.rangoFechas(fechaIni,fechaFin,"%d-%m-%Y")),moneda, admin



class main:
    def main():
        try:
            logging.debug(sys.argv)
            fechas,moneda, admin = recibirParametros.get()
            cotizaciones = {}
            i = 0
            for fecha in fechas:
                cotizaciones[fecha] = obtenedor.cotizacion(fecha,moneda).json()
                i = i+1 
                if i == 50:
                    time.sleep(60) 
                    #Si ya hicimos 50 llamados deja pasar un minuto para no tener problemas con la API
                    i = 0
            admin.guardar(fechas[0]+"_to_"+fechas[-1],moneda,cotizaciones)
        except BaseException as e:
            logging.critical(e)
main.main()



