import psycopg2


def get_transition_matrix(cursor):
    matrix = {}

    # Получаем все уникальные id веб-страниц
    pages = get_pages(cursor)

    # Создаем матрицу заполненные нулями в словарном представлений
    # Например, {'A' : {'A':0, 'B':0, 'C':0}
    #            'B' : {'A':0, 'B':0, 'C':0}
    #            'C' : {'A':0, 'B':0, 'C':0}}
    destination_ids = {}
    for page in pages:
        destination_ids[page] = 0.0

    for page in pages:
        matrix[page] = destination_ids.copy()

    # Заполняем матрицу переходов
    for page in pages:
        cursor.execute("""select destination_id from transition where source_id = '{0}'""".format(page))
        destin_ids = [r[0] for r in cursor.fetchall()]
        if len(destin_ids) == 0:
            value = 0
        else:
            value = 1/len(destin_ids)
        for d_id in destin_ids:
            matrix[page][d_id] = value

    return matrix


def get_stationary_vector(cursor):
    cursor.execute("""select count(page_id) from pages;""")
    count = cursor.fetchall()[0][0]
    vector = []
    for i in range(count):
        vector.append(1/count)
    return vector


def multiply_matrix_by_vector(matrix, vector, pages):
    count1 = 0
    result_vector = []
    for page1 in pages:
        vector_value = 0
        count2 = 0
        for page2 in pages:
            vector_value += matrix[page2][page1] * vector[count2]
            count2 += 1
        result_vector.append(vector_value)
        count1 += 1
    return result_vector


def get_pages(cursor):
    cursor.execute("""select page_id from pages order by page_id;""")
    return [r[0] for r in cursor.fetchall()]


def main():
    connection = psycopg2.connect(
        database="postgres",
        user="postgres",
        password="mbr2010IL",
        host="database-3-instance-1.c0sw2mz3atbl.us-east-1.rds.amazonaws.com",
        port='5432'
    )
    cursor = connection.cursor()
    transition_matrix = get_transition_matrix(cursor)
    vector = get_stationary_vector(cursor)
    pages = get_pages(cursor)
    for i in range(23):
        vector = multiply_matrix_by_vector(transition_matrix,
                                           vector,
                                           pages)
    sum = 0
    for e in vector:
        sum += e
    print('Hello')
    print(vector)

    for i in range(0, len(vector)):
        cursor.execute("""UPDATE pages SET rank = '{0}' WHERE page_id = '{1}'""".format(vector[i], pages[i]))
        cursor.commit()
        print('done')


if __name__ == '__main__':
    main()
