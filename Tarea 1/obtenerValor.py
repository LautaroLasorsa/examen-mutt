from abc import abstractmethod
import sys
from xml.dom.pulldom import ErrorHandler
from psutil import Error
import requests
import datetime
import logging
import psycopg2

logging.basicConfig(level=logging.WARNING)

class obtenedor:
    def cotizacion(fecha,moneda):
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
        nombreArchivo = moneda+fecha+".json"
        archivo = open(nombreArchivo,'w')
        archivo.write(cotizacion)

class administradorPostgres(administrador):
    
    nombre = "postgres"
    usuario = "postgres"
    password = "mysecretpassword"
    host = "localhost"
    puerto = "8080"
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
        print(self.password)
        self.conexion = psycopg2.connect(
            dbname = self.nombre, user = self.usuario, 
            password = self.password, host = self.host,
            port = self.puerto)
        self.cursor = self.conexion.cursor()
        self.chequeoInicio()

    def chequeoInicio(self):
        try:
            self.cursor.execute(
            "SELECT *  FROM INFORMATION_SCHEMA.TABLES\
            WHERE TABLE_NAME = 'full_data';")
            
            if(len(self.cursor.fetchall())==0):
            
                self.cursor.execute("CREATE TABLE full_data( coin_id char(100), price money , date date ,json char(7000) );")
                self.conexion.commit()
            
            self.cursor.execute(
                "SELECT *  FROM INFORMATION_SCHEMA.TABLES\
                WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'agregate_data';")
            
            if(len(self.cursor.fetchall())==0):
                self.cursor.execute("CREATE TABLE agregate_data( coin_id char(100), year int , month int , max_price money, min_price money );")
                self.conexion.commit()
            
                
        except BaseException as e:
            print(e)


    def guardar(self,fecha,moneda,cotizacion):
        print("postgres")



class recibirParametros:
    def get():
        if(len(sys.argv)<3):
            logging.critical("Too few params\n")
            exit()
        try:
            postgresi = sys.argv.index('postgres')
            admin = administradorPostgres(*sys.argv[postgresi+1:])
            sys.argv = sys.argv[:postgresi]
        except:
            print("no aparece")
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
                print(sys.argv)
                logging.critical("The first and second arguments have to be a date\n")
                exit()
            moneda = sys.argv[3]
            
        return list(rangoFechas.rangoFechas(fechaIni,fechaFin,"%d-%m-%Y")),moneda, admin



class main:
    def main():
        fechas,moneda, admin = recibirParametros.get()
        cotizaciones = {
            fecha:obtenedor.cotizacion(fecha,moneda).json() for fecha in fechas
        }
        admin.guardar(fechas[0]+"_to_"+fechas[-1],moneda,str(cotizaciones))

main.main()



