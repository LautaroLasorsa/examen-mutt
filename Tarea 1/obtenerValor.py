from abc import abstractmethod
import sys
from numpy import void
import requests
import datetime
import logging

logging.basicConfig(level=logging.WARNING)

class obtenedor:
    def cotizacion(fecha,moneda):
        return requests.get("https://api.coingecko.com/api/v3/coins/{}/history?date={}".format(moneda,fecha))

class rangoFechas:
    def rangoFechas(fechaIni, fechaFin, formato = void):
        fecha = fechaIni
        for d in range((fechaFin-fechaIni).days+1):
            if formato!=void:
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
    def guardar(self,fecha,moneda,cotizacion):
        print("postgres")

class recibirParametros:
    def get():
        if(len(sys.argv)<3):
            logging.critical("Too few params\n")
            exit()
        
        if(sys.argv[-1]=='postgres'):
            admin = administradorPostgres()
            print(sys.argv)
        else:
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
                logging.critical("The first and second arguments have to be a date\n")
                exit()
            moneda = sys.argv[3]
            
        return list(rangoFechas.rangoFechas(fechaIni,fechaFin,"%d-%m-%Y")),moneda, admin



class main:
    def main():
        fechas,moneda, admin = recibirParametros.get()
        if len(fechas)==1:
            cotizacion = obtenedor.cotizacion(fechas[0],moneda)
            admin.guardar(fechas[0],moneda,cotizacion.text)
        else:
            cotizaciones = {
                fecha:obtenedor.cotizacion(fecha,moneda).json() for fecha in fechas
            }
            admin.guardar(fechas[0]+fechas[-1],moneda,str(cotizaciones))

main.main()



