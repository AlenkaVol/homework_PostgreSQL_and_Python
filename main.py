import psycopg2


# Функция, удаляющая таблицы
def delete_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
            DROP TABLE phone_number;
            DROP TABLE personal_information;
        """)
        conn.commit()
    print('Таблицы phone_number и personal_information удалены')

# Функция, создающая структуру БД (таблицы)
def create_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS personal_information(
                id SERIAL PRIMARY KEY,
                first_name VARCHAR (60) NOT NULL,
                last_name VARCHAR (60) NOT NULL,
                email VARCHAR(80) UNIQUE NOT NULL
            );
            """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS phone_number(
                id SERIAL PRIMARY KEY,
                personal_id INTEGER NOT NULL REFERENCES personal_information(id),
                number BIGINT NOT NULL CHECK (number>1)
            );
            """)
        conn.commit()
    print('Таблицы phone_number и personal_information созданы')

# Функция для получения списка всех email
def get_emails(conn):
    list_emails = []
    with conn.cursor() as cur:
        cur.execute("""
            SELECT email FROM personal_information;
            """)
        for email in cur.fetchall():
            list_emails.append(email[0])
    return list_emails

# Функция, позволяющая добавить нового клиента
def add_client(conn, first_name, last_name, email, phones=None):
    if email not in get_emails(conn):
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO personal_information(first_name, last_name, email) VALUES(%s, %s, %s)
                RETURNING id;
                """, (first_name, last_name, email))
            personal_id = cur.fetchone()[0]

            if phones is not None:
                phones_str = str(phones)
                for phone in phones_str.split(', '):
                    cur.execute("""
                        INSERT INTO phone_number(personal_id, number) VALUES(%s, %s);
                        """, (personal_id, int(phone)))
            conn.commit()
            print(f'Клиент {first_name} {last_name} добавлен')
    else:
        print(f'Клиент с почтой {email} уже существует')

# Функция для получения списка телефонный номеров
def get_phone_numbers(conn):
    list_phone_numbers = []
    with conn.cursor() as cur:
        cur.execute("""
            SELECT number FROM phone_number;
            """)
        for number in cur.fetchall():
            list_phone_numbers.append(number[0])
    return list_phone_numbers

# Функция для получения id всех клиентов
def get_clients_id(conn):
    list_clients_id = []
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id FROM personal_information;
            """)
        for id in cur.fetchall():
            list_clients_id.append(id[0])
    return list_clients_id

# Функция, позволяющая добавить телефон для существующего клиента
def add_phone(conn, client_id, phone):
    if client_id in get_clients_id(conn):
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO phone_number(personal_id, number) VALUES(%s, %s);
                """, (client_id, phone))
            conn.commit()
            print(f'Номер телефона {phone} для клиента {client_id} добавлен')
    else:
        print(f'Клиента с номером {client_id} не существует')

# Функция, позволяющая изменить данные о клиенте
def change_client(conn, client_id, first_name=None, last_name=None, email=None, last_phone=None, new_phone=None):
    if client_id in get_clients_id(conn):
        with conn.cursor() as cur:
            if first_name is not None:
                cur.execute("""
                    UPDATE personal_information SET first_name=%s WHERE id=%s;
                    """, (first_name, client_id))
                conn.commit()
                print(f'Имя для клиента {client_id} изменено на {first_name}')

            if last_name is not None:
                cur.execute("""
                    UPDATE personal_information SET last_name=%s WHERE id=%s;
                    """, (last_name, client_id))
                conn.commit()
                print(f'Фамилия для клиента {client_id} изменена на {last_name}')

            if email is not None:
                if email not in get_emails(conn):
                    cur.execute("""
                        UPDATE personal_information SET email=%s WHERE id=%s;
                        """, (email, client_id))
                    conn.commit()
                    print(f'Email для клиента {client_id} изменен на {email}')
                else:
                    print(f'Почта {email} уже существует в базе данных')

            if new_phone is not None:
                if last_phone is not None:
                    if last_phone in get_phone_numbers(conn):
                        cur.execute("""
                            UPDATE phone_number SET number=%s WHERE id=%s AND number=%s;
                            """, (new_phone, client_id, last_phone))
                        conn.commit()
                        print(f'Номер телефона {last_phone} для клиента {client_id} изменен на {new_phone}')
                    else:
                        print(f'Номера телефона {last_phone} нет в базе данных')
                else:
                    print('Укажите номер телефона, который необходимо поменять на новый')
    else:
        print(f'Клиента с номером {client_id} не существует')

# Функция, позволяющая удалить телефон для существующего клиента
def delete_phone(conn, client_id, phone):
    if client_id in get_clients_id(conn):
        if phone in get_phone_numbers(conn):
            with conn.cursor() as cur:
                cur.execute("""
                    DELETE FROM phone_number WHERE personal_id=%s AND number=%s;
                    """, (client_id, phone))
                conn.commit()
            print(f'Телефонный номер {phone} удалён из базы данных')
        else:
            print(f'Телефонного номера {phone} нет в базе данных')
    else:
        print(f'Клиента с номером {client_id} не существует')

# Функция, позволяющая удалить существующего клиента
def delete_client(conn, client_id):
    if client_id in get_clients_id(conn):
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM phone_number WHERE personal_id=%s;
                """, (client_id,))
            cur.execute("""
                DELETE FROM personal_information WHERE id=%s;
                """, (client_id,))
            conn.commit()
        print(f'Клиент под номером {client_id} удалён из базы данных')
    else:
        print(f'Клиента с номером {client_id} не существует')

# Функция, позволяющая найти клиента по его данным: имени, фамилии, email или телефону
def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT pi.id, first_name, last_name, email, number FROM personal_information pi 
            LEFT JOIN phone_number pn ON pi.id = pn.personal_id
            WHERE first_name=%s OR last_name=%s OR email=%s OR number=%s;
            """, (first_name, last_name, email, phone))
        return cur.fetchall()


with psycopg2.connect(database="clients_db", user="postgres", password="postgres") as conn:
    # создаем БД (таблицы)
    create_db(conn)

    # добавляем новых клиентов
    # add_client(conn, 'Иван', 'Иванов', 'inav.i@mail.ru')
    # add_client(conn, 'Петр', 'Соколов', 'petr.s@gmail.com', phones=89245618793)
    # add_client(conn, 'Ольга', 'Кузнецова', 'olya.k@mail.ru', phones='89148479011, 89140097011')

    # добавляем телефон для клиента
    # add_phone(conn, 1, 79245566777)

    # меняем данные клиента
    # change_client(conn, 3, last_name='Романова')
    # change_client(conn, 3, new_phone=89098444446, last_phone=89140097011)

    # удаляем телефон клиента
    # delete_phone(conn, 2, 89245618793)

    # удаляем клиента из БД
    # delete_client(conn, 1)

    # ищем клиента по его данным
    # print(find_client(conn, email='petr.s@gmail.com'))
    # print(find_client(conn, phone=89098444446))

    # удаляем БД
    # delete_db(conn)

conn.close()