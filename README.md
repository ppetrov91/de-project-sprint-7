# Проект 7-го спринта

### Описание переноса данных
1. Перед расчётом витрин нужно перенести данные из /user/master/data/geo/events в
   /user/sppetrov12/data/events. При этом данные разбиты на партиции сначала по дате и времени события, а
   затем по его типу. 

   /user/sppetrov12/data/events/date=2022-05-31/event_type=message
   /user/sppetrov12/data/events/date=2022-05-31/event_type=subscription
   /user/sppetrov12/data/events/date=2022-05-31/event_type=reaction

2. В geo.csv был добавлен столбец timezone, с помощью которого можно будет определить локальное время города,
   в котором произошло событие.

3. Сам файл geo.csv находится в /user/sppetrov12/data/geo.csv.

4. Во время переноса данных происходит привязывание координат события к городу с наименьшим расстонием в километрах. Постоянное выполнение cross join во время построения витрин является крайне неэффективным.

   Если у события есть широта и долгота, то во время переноса к нему привязывается город.

   Если у события нет либо широты, либо долготы, то во вместо города и временно зоны выставляется символ "-".

5. Таким образом, к структуре events добавляются четыре поля:
     - city - город события.
     - timezone - временная зона города события.
     - datetime - дата и время возникновения события. Если event.datetime не определён, 
                  то берётся message_ts (дата и время сообщения)
                
     - user_id - пользователь, совершивший событие (подписка, отправка сообщения, реакция).

6. Для удобства, поле lat перименовано на event_lat, а lon - на event_lon.

7. Задача загрузки событий представлена в файле src/scripts/load_events_job.py.
  
   Ниже приведены параметры запуска:
     - dt - дата, до которой включительно будет загрузка сообщений.
	 - depth - глубина в днях. Т.е, события будут загружаться на отрезке [dt - depth; dt].
	 - hdfs_url - URL к hdfs.
	 - master - способ подключения к Spark (local[*], yarn).
	 - uname - логин пользователя (sppetrov12).
	 - events_src_path - путь, из которого брать данные по events.
	 - events_dst_path - путь, в которой будут сохранены даные по events после преобразования.
	 - city_path - путь к geo.csv на hdfs. 

8. DAG-запуска задачи загрузки событий представлен в файле scr/dags/load_events_dag.py.


### Описание users_datamart
1. 
2. 

### Описание zones_datamart
1. 
2.

### Описание recommedations_datamart