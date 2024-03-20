# Проект 7-го спринта

### Описание переноса данных
1. Перед расчётом витрин нужно перенести данные из /user/master/data/geo/events в
   /user/sppetrov12/data/events. При этом данные разбиты на партиции сначала по дате и времени события, а
   затем по его типу. 

   - /user/sppetrov12/data/events/date=2022-05-31/event_type=message
   - /user/sppetrov12/data/events/date=2022-05-31/event_type=subsrciption
   - /user/sppetrov12/data/events/date=2022-05-31/event_type=reaction

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

7. Задача загрузки событий представлена в файле src/srcipts/load_events_job.py.
  
   Ниже приведены параметры запуска:
     - dt - дата, до которой включительно будет загрузка сообщений.
	 - depth - глубина в днях. Т.е, события будут загружаться на отрезке [dt - depth; dt].
	 - hdfs_url - URL к hdfs.
	 - master - способ подключения к Spark (local[*], yarn).
	 - uname - логин пользователя (sppetrov12).
	 - events_src_path - путь, из которого брать данные по events.
	 - events_dst_path - путь, в которой будут сохранены даные по events после преобразования.
	 - city_path - путь к geo.csv на hdfs. 

8. DAG-запуска задачи загрузки событий представлен в файле src/dags/load_events_dag.py.

### Описание файла utils.py
1. input_paths - функция, возвращающая пути с данными. Принимает следующие аргументы:
     - sc - объект типа SparkContext.
	 - hdfs_url - URL к hdfs.
	 - input_path - путь к данным типа events.
	 - dt - дата, до которой включительно будет загрузка сообщений.
	 - depth - глубина в днях. Т.е, события будут загружаться на отрезке [dt - depth; dt].
	 - event_type - тип события, необязательный параметр. По умолчанию, будут выбираться все события.

   Для ускорения выборки идёт обращение к партициям, но если хотя бы одного пути не будет в hdfs, то

   скрипт обработки завершится с ошибкой. Поэтому при формировании списка путей к данными проверяется наличие того или иного пути в hdfs

2. get_events - функция, читающая данные в формате parquet и возвращающая указанные выражения.

   Принимает следующие аргументы:
     - sc - объект типа SparkContext.
	 - s - объект типа SparkSession.
	 - hdfs_url - URL к hdfs.
	 - events_src_path - путь к данным типа events.
	 - dt - дата, до которой включительно будет загрузка сообщений.
	 - depth - глубина в днях. Т.е, события будут загружаться на отрезке [dt - depth; dt].
	 - expr_list - список выражений в select, требуются не все данные events, а только часть из них.
	 - event_type - тип события, необязательный параметр. По умолчанию, будут выбираться все события.
   
### Описание users_datamart
1. Реализация представлена в файле src/srcipts/users_datamart_job.py.

   Ниже приведены параметры запуска:
     - dt - дата, до которой включительно будет загрузка сообщений.
	 - depth - глубина в днях. Т.е, события будут загружаться на отрезке [dt - depth; dt].
	 - hdfs_url - URL к hdfs.
	 - master - способ подключения к Spark (local[*], yarn).
	 - uname - логин пользователя (sppetrov12).
	 - events_src_path - путь, из которого брать преобразованные данные по events.
	 - users_datamart_base_path - путь, в который будут сохранены рассчитанные данные витрины.

2. Данные витрины хранятся по пути /user/sppetrov12/data/analytics/users_datamart/date=dt/depth=dp

3. act_city - город, в котором пользователь отправил своё последнее сообщение.

   В данном случае, множество событий разбивается на группы по user_id.

   Каждая группа упорядочивается по полю datetime в порядке возрастания.

   Таким образом, для получения города нужно использовать функцию last_value.

   Здесь нужно оставить для каждого пользователя только одну строку.

   В данном случае, это строка с номером, равным количеству строк в группе.

4. Для определения маршрута пользователя нужно учитывать все события, а не только сообщения.

   Здесь множество событий также разбивается на группы по user_id.

   Каждая группа упорядочивается по полю datetime в порядке возрастания.	

   Для каждого события в группе определяется следующий город и дата следующего события c помощью функции lead.

   Нас интересуют только те строки, у которых текущий город не равен следующему.

   Если у текущего города нет следующего города, то он также остаётся в выборке.

   Полученная выборка позволит определить маршрут в порядке посещения, а также localtime.

   localtime - локальное время последнего события для каждого пользователя.

5. Для определения домашнего города нужно вычесть из даты следующего события дату текущего события.

   Если у текущего события нет следующего, то дата следующего события равна дате текущего.

   Это позволяет нам получить разность дат в виде дней.

   Если пользователь посетил только один город, то он считается домашним.

   Если пользователь посетил несколько городов, то домашний город - город, в котором пользователь провёл более 27 дней. Если таких городов несколько, то выбирается город, посещённый пользователем последним.

6. Финальная выборка получается путём соединения данных о маршруте с данными об актуальном и домашнем городе.

   Тип соединения left, поскольку пользователь за выбранном отрезок времени может не отправлять сообщения, поэтому у него может не быть актуального города.

   Также, пользователь мог находиться во всех городах маршрута менее 27 дней, поэтому домашнего города также может и не быть.

7. DAG-запуска задачи загрузки событий представлен в файле src/dags/users_datamart_dag.py.

### Описание zones_datamart
1. Реализация представлена в файле src/srcipts/zones_datamart_job.py.

   Ниже приведены параметры запуска:
     - dt - дата, до которой включительно будет загрузка сообщений.
	 - depth - глубина в днях. Т.е, события будут загружаться на отрезке [dt - depth; dt].
	 - hdfs_url - URL к hdfs.
	 - master - способ подключения к Spark (local[*], yarn).
	 - uname - логин пользователя (sppetrov12).
	 - events_src_path - путь, из которого брать преобразованные данные по events.
	 - zones_datamart_base_path - путь, в который будут сохранены рассчитанные данные витрины.

2. Данные витрины хранятся по пути /user/sppetrov12/data/analytics/zones_datamart/date=dt/depth=dp.

3. Здесь datetime урезается до начала месяца и недели соответственно, в результате, появляются значения для полей month и week.

   Сначала исходное множество группируется по полям month и city.

   А затем исходное множество группируется по полям month, week и city.

   Затем эти множества соединяются с помощью join и формируют конечную выборку.

   Опытным путём было выяснено, что данный вариант работал быстрее применения оконной функции по month и city

   и последующей агрегации по month, week и city.

4. DAG-запуска задачи загрузки событий представлен в файле src/dags/zones_datamart_dag.py.

### Описание recommedations_datamart
1. Реализация представлена в файле src/srcipts/recommendations_datamart_job.py.

   Ниже приведены параметры запуска:
     - dt - дата, до которой включительно будет загрузка сообщений.
	 - depth - глубина в днях. Т.е, события будут загружаться на отрезке [dt - depth; dt].
	 - hdfs_url - URL к hdfs.
	 - master - способ подключения к Spark (local[*], yarn).
	 - uname - логин пользователя (sppetrov12).
	 - events_src_path - путь, из которого брать преобразованные данные по events.
	 - recommendations_datamart_base_path - путь, в который будут сохранены рассчитанные данные витрины.

2. Данные витрины хранятся по пути /user/sppetrov12/data/analytics/recommendations_datamart/date=dt/depth=dp.

3. Если пользователи подписаны на один канал, то их можно по нему сгруппировать.

   В данном случае, это делается с помощью 
   
   Window().partitionBy(["subsrciption_channel"]).orderBy(F.col("user_id"))

   Пары формируются с помощью функции lead(). Опытным путём было выяснено, что это работает быстрее, чем

   соединять выборку саму с собой с помощью join.

4. Затем нужно выбрать за указанный временной отрезок все события типа message, у которых 

   определены message_from и message_to. Их нужно объединить для получения пар пользователей, которые писали дргу другу.

5. Полученные пары подписок соединяются с множеством сообщений типом anti, поскольку нам нужны пары пользователей, которые друг другу не писали.

6. Для каждого пользователя и каждого типа события определяются последние события по datetime.

   Возможно, надо делать так, поскольку рекомендации поступают после того, как пользователь что-то сделал.

   Полученные в пункте 5 пары соединяются с последними событиями, для которых определены широта и долгота.

   Получим координаты событий для пары пользователей, оставляем только те пары, у которых расстояние между ними < 1 км.

   Они и составят финальную выборку.

7. DAG-запуска задачи загрузки событий представлен в файле src/dags/recommendations_datamart_dag.py.