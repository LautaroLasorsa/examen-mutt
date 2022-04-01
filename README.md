Script: obtenerValor.py

El escript recibe 1 o 2 fechas y el nombre de una moneda.

Por ejemplo:

- obtenerValor.py 2022-03-05 bitcoin
- obtenerValor.py 2022-02-05 2022-03-05 ethereum

Si recibe una sola fecha, tomara esta como fecha de inicio y fin de la consulta.
Si recibe dos fechas, las tomara como inicio y fin de la consulta (detecta
automaticamente cuál es anterior, pero advierte si se pasaron en el orden inverso)

Generará un archivo llado moneda_from_fechaInicio_to_fechaFin donde se guardará un
JSON indexado por la fecha, y para cada fecha el valor relacionado será el resultado
de llamar a la API en dicha fecha para dicha moneda.

Si se agrega el parametro postgres al final en lugar de guardar un archivo cargará los
datos en la base de datos postgres SQL que contiene las siguientes tablas (las crea
si no existen):

- full_data: que contiene para cada moneda y fecha la cotización en USD y el JSON 
  resultado de llamar a la API en esa fecha y moneda.
- agregate_data: que contiene para cada moneda, año y mes la máxima y mínima cotización
  en USD en dicho mes.

Como la API tiene un límite de 50 llamados por minuto, y para cada moneda y fecha hay que
hacer un llamado distinto, si ya hizó 50 llamados el programa dejará pasar un minuto antes
de seguir consultando a la API. 

Para conectarse a la base de datos el script llama a la función psycopg2.connect, que recibe
5 parametros. Si al llamar al script luego de postgres agregamos más valores, sobre escribiremos
estos parametros. 

Lista de parametros y sus valores por defecto:
dbname   = "postgres"
user     = "postgres"
password = "mysecretpassword"
host     = "localhost"
port     = "5432"

Comando recomendado para crear la base de datos:
sudo docker run --name mi-postgres -e POSTGRES_PASSWORD=mysecretpassword -p 5432:5432 -d postgres

Comando recomendado para ejecutar la base de datos ya creada:
sudo docker start mi-postgres

Comando recomendado para navegar dentro de la base de datos de forma manual:
sudo docker exec -it -u postgres mi-postgres /bin/sh