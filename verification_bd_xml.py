# _*_ coding: utf-8 _*_
import psycopg2
import logging

"""Logging"""
# получение пользовательского логгера и установка уровня логирования
py_logger = logging.getLogger(__name__)
py_logger.setLevel(logging.INFO)

# настройка обработчика и форматировщика в соответствии с нашими нуждами
py_handler = logging.FileHandler(f"{'Проверка  BD с XML'}.log", mode='w')
py_formatter = logging.Formatter("%(message)s")

# добавление форматировщика к обработчику
py_handler.setFormatter(py_formatter)

# добавление обработчика к логгеру
py_logger.addHandler(py_handler)

DATABASE = 'aos'

"""Проверка наличия больше одного родительского learning_node_id."""


def connectDB():
    """Подключение к базе"""
    print('Connect database', DATABASE)
    try:
        connect = psycopg2.connect(
            database=DATABASE,
            user="aosadmin",
            password="oasndqimlli",
            host="192.168.1.232",
            port="5432"
        )
        print("Database open")
        return connect
    except AttributeError:
        print('Can`t establish connection to database')


def getBlock(select_sql_block):
    """Получить список блоков"""
    learning_node_id_list = []
    cursor.execute(select_sql_block)
    all_block_id = cursor.fetchall()
    learning_node_id_list.append(all_block_id[0][0])
    return learning_node_id_list


def getStep_id(block_id, select_sql):
    """Получить список шагов"""
    stepList = []
    for i in block_id:
        for id in i:
            select_step = select_sql
            cursor.execute(select_step, {'step_id': id})
            all_step_id = cursor.fetchall()
            stepList.append(all_step_id[0][0])
    return stepList


def getParent(step_id, select_sql):
    """Поличить родителя"""
    cursor = connect.cursor()
    parentList = []
    for n_child in step_id:
        for child in n_child:
            select_child = select_sql
            cursor.execute(select_child, {'child_id': child})
            all_parent = cursor.fetchall()

            if all_parent[0][0] > 1:
                parentList.append(child)
    return parentList


def getBlockAndStepXml(select_sql):
    """Получить список блоков"""
    list_xml_bd = []
    cursor.execute(select_sql)
    list_xml_bd = cursor.fetchall()
    list_xml_bd.append(list_xml_bd[0])
    return list_xml_bd


def comparisonXmlVsBd(list_xml_bd):
    """Сравнить XML и BD"""
    step_xml = []
    frame_xml = []
    step_bd = []
    frame_bd = []
    total = [[0], [0], [0], [0]]
    count = 0
    while count != len(list_xml_bd):
        for i in range(len(list_xml_bd[count])):
            if i == 0 and list_xml_bd[count][0] is None:
                step_xml.append(list_xml_bd[count])
            elif i == 1 and list_xml_bd[count][1] is None:
                frame_xml.append(list_xml_bd[count])
            elif i == 2 and list_xml_bd[count][2] is None:
                step_bd.append(list_xml_bd[count])
            elif i == 3 and list_xml_bd[count][3] is None:
                frame_bd.append(list_xml_bd[count])
        count += 1
    total[0] = step_xml
    total[1] = frame_xml
    total[2] = step_bd
    total[3] = frame_bd

    return total


def closeDB(connect):
    """Закрыть соединение с курсором и базой данных"""
    connect.close()
    cursor.close()
    print("Database close")
    print("Cursor close")


if __name__ == '__main__':
    connect = connectDB()
    cursor = connect.cursor()

    select_sql_block = """select array(select learning_node_id 
                        from q_learning q 
                              where node_type_id = 4 and q.deleted is null)"""  # where learning_node_id in (104964))"""

    select_sql_step = """select array(select learning_node_child_id 
                            from l_learning_structure ll 
                            inner join q_learning ql on ql.learning_node_id = ll.learning_node_child_id 
                            where ll.learning_node_id = %(step_id)s and ql.deleted is null order by 1)"""

    select_sql_parent = """select count (learning_node_id) 
                    from l_learning_structure 
                    where learning_node_child_id = %(child_id)s"""

    select_sql_xml = """with cnt as (
                          select convert_from(content, 'utf8')::xml res
                          from q_learning_content qlc
                          inner join q_learning ql on ql.learning_node_id = qlc.learning_node_id  
                          where  content_type_id = 101 and ql.deleted is null
                        )
                        , tbl as (
                          select xmltable.*
                          from cnt,
                          xmltable('/Block/steps/step/*/node'
                               passing res
                               columns 
                               step_id_xml text path '../../@id',
                               frame_id_xml int path '@frame_id')
                        )
                        --select * from tbl where step_id_xml::int = 93659
                        , block as (
                            select learning_node_id
                                from q_learning ql
                            where node_type_id = 4 and ql.deleted is null --and learning_node_id = 93658
                        
                        )
                        , step as (
                            select ll.learning_node_child_id step_id_bd  
                                from l_learning_structure ll
                                    inner join block using(learning_node_id)
                                    inner join q_learning ql on ql.learning_node_id = ll.learning_node_child_id
                                where ql.deleted is null
                            
                        )
                        --select * from step
                        ,step_frame as(
                            select step_id_bd, ls.learning_node_child_id frame_id_bd
                                from step s
                                    inner join l_learning_structure ls on ls.learning_node_id = s.step_id_bd
                                    inner join q_learning ql on ql.learning_node_id = ls.learning_node_child_id
                                where ql.deleted is null
                        )
                        
                        , xml_bd as (
                            select tbl.step_id_xml, tbl.frame_id_xml, s.step_id_bd, s.frame_id_bd 
                                from tbl
                            full outer join step_frame s on s.step_id_bd = tbl.step_id_xml::int and s.frame_id_bd = tbl.frame_id_xml
                            --full outer join step_frame sf on 
                        )
                        select step_id_xml, frame_id_xml, step_id_bd, frame_id_bd 
                            from xml_bd
                            --where step_id_bd is not null
                        group by step_id_xml, frame_id_xml, step_id_bd, frame_id_bd """

    try:
        RESULT = 'PASS'
        block_id = getBlock(select_sql_block)
        step_id = getStep_id(block_id, select_sql_step)
        parent_step = getParent(step_id, select_sql_parent)

        if len(parent_step) != 0:
            RESULT = 'FAIL'
        py_logger.info(f"Test 1: {'Шаг привязян к нескольким блокам!'} | {RESULT}")
        for s in set(parent_step):
            py_logger.info(f" id step: {s}")
        py_logger.info(f"{'_____________________________________________________________'}")

        frame_id = getStep_id(step_id, select_sql_step)
        parent_frame = getParent(frame_id, select_sql_parent)

        if len(parent_frame) != 0:
            RESULT = 'FAIL'
        py_logger.info(f"Test 2: {'Кадр привязан к нескольким шагам!'} | {RESULT}")
        for p in set(parent_frame):
            py_logger.info(f" id frame: {p}")
        py_logger.info(f"{'_____________________________________________________________'}")

        """Сравить BD и XML"""

        xml_list = getBlockAndStepXml(select_sql_xml)
        total = comparisonXmlVsBd(xml_list)

        test_list = {'info': {0: "Есть шаг (id) в БД, но отсутствует в XML (id)!",
                              1: "Есть кадр (id) в БД, но отсутствует в XML (id)!",
                              2: "Есть шаг (id) в XML, но отсутствует в БД (id)!",
                              3: "Есть кадр (id) в XML, но отсутствует в БД (id)!"
                              },
                     'columns': {0: f"{'Step_xml'}\t{'Step_bd'}",
                                 1: f"{'Frame_xml'}\t{'Frame_bd'}",
                                 2: f"{'Step_xml'}\t{'Step_bd'}",
                                 3: f"{'Frame_xml'}\t{'Frame_bd'}",
                                 },
                     }
        count = 3

        for index in range(len(total)):

            if len(total[index]) != 0:
                RESULT = 'FAIL'

            py_logger.info(f"Test {count}: {test_list['info'][index]} | {RESULT}")
            py_logger.info(f"{test_list['columns'][index]}")

            count += 1
            for i in total[index]:
                if index == 0:
                    py_logger.info(f"{i[0]}\t\t{i[2]}")
                if index == 1:
                    py_logger.info(f"{i[1]}\t\t{i[3]}")
                if index == 2:
                    py_logger.info(f"{i[0]}\t\t{i[2]}")
                if index == 3:
                    py_logger.info(f"{i[1]}\t\t{i[3]}")
            py_logger.info(f"{'_____________________________________________________________'}")

        closeDB(connect)

    except AttributeError as att:
        print("Ошибка в работе программы!")

    finally:
        closeDB(connect)
