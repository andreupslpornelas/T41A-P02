import psycopg2
import pytest

@pytest.fixture(scope="module")
def db_connection():
    """
    Establece una conexión con la base de datos de prueba y se asegura de que esté
    poblada con los datos de prueba antes de que se ejecuten las pruebas.
    """
    conn = psycopg2.connect(
        dbname="test_db",
        user="postgres",
        password="postgres",
        host="localhost",
        port="5432"
    )
    with conn.cursor() as cur:
        # Inserción de datos de prueba
        cur.execute("""
            -- Inserción de carreras
            INSERT INTO carrera (nombre) VALUES
            ('Ingeniería en Tecnologías de la Información');

            -- Inserción de alumnos
            INSERT INTO alumno (matricula, nombre, apellido) VALUES
            ('A001', 'Ana', 'Torres'),
            ('A002', 'Luis', 'Gómez'),
            ('A003', 'María', 'López'),
            ('A004', 'Carlos', 'Ruiz'),
            ('A005', 'Laura', 'Méndez'),
            ('A006', 'Pedro', 'Sánchez'),
            ('A007', 'Sofía', 'Díaz'),
            ('A008', 'Jorge', 'Ramírez'),
            ('A009', 'Elena', 'Castro'),
            ('A010', 'Tomás', 'Ortega');

            -- Inserción de maestros
            INSERT INTO maestro (nombre, apellido) VALUES
            ('Juan', 'Pérez'),
            ('Carmen', 'Silva'),
            ('Diego', 'Luna'),
            ('Rosa', 'Márquez'),
            ('Andrés', 'Bello'),
            ('Julia', 'Ríos'),
            ('Sergio', 'Peña'),
            ('Alicia', 'Torres'),
            ('Iván', 'Cordero'),
            ('Teresa', 'León');

            -- Inserción de grupos
            INSERT INTO grupo (clave, materia, periodo, id_maestro, id_carrera) VALUES
            ('T41A', 'Bases de Datos I', '20253S', 1, 1),
            ('T41B', 'Bases de Datos I', '20253S', 2, 1),
            ('T42A', 'Bases de Datos II', '20253S', 3, 1),
            ('T42B', 'Bases de Datos II', '20253S', 4, 1),
            ('T43A', 'Diseño de BD', '20253S', 5, 1),
            ('T43B', 'Diseño de BD', '20253S', 6, 1),
            ('T44A', 'SQL Avanzado', '20253S', 7, 1),
            ('T44B', 'SQL Avanzado', '20253S', 8, 1),
            ('T45A', 'PostgreSQL', '20253S', 9, 1),
            ('T45B', 'PostgreSQL', '20253S', 10, 1);

            -- Inserción de inscripciones
            INSERT INTO inscripcion (id_alumno, id_grupo) VALUES
            (1, 1), (2, 1), (3, 2), (4, 2), (5, 3), (6, 3), (7, 4), (8, 4), (9, 5), (10, 5);

            -- Inserción de asistencias
            INSERT INTO asistencia (id_inscripcion, presente) VALUES
            (1, TRUE), (2, FALSE), (3, TRUE), (4, FALSE), (5, TRUE), (6, FALSE), (7, TRUE), (8, FALSE), (9, TRUE), (10, FALSE);
        """)
    conn.commit()
    yield conn
    # Limpia la base de datos después de las pruebas para dejarla en un estado limpio
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE asistencia, inscripcion, grupo, maestro, alumno, carrera RESTART IDENTITY CASCADE;")
    conn.commit()

def test_carreras_insertadas(db_connection):
    with db_connection.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM carrera;")
        count = cur.fetchone()[0]
        assert count == 1, "Se esperaba 1 carrera insertada"

def test_alumnos_insertados(db_connection):
    with db_connection.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM alumno;")
        count = cur.fetchone()[0]
        assert count == 10, "Se esperaban 10 alumnos insertados"

def test_maestros_insertados(db_connection):
    with db_connection.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM maestro;")
        count = cur.fetchone()[0]
        assert count == 10, "Se esperaban 10 maestros insertados"

def test_grupos_insertados(db_connection):
    with db_connection.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM grupo;")
        count = cur.fetchone()[0]
        assert count == 10, "Se esperaban 10 grupos insertados"

def test_inscripciones_insertadas(db_connection):
    with db_connection.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM inscripcion;")
        count = cur.fetchone()[0]
        assert count == 10, "Se esperaban 10 inscripciones insertadas"

def test_asistencia_insertada(db_connection):
    with db_connection.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM asistencia;")
        count = cur.fetchone()[0]
        assert count == 10, "Se esperaban 10 registros de asistencia insertados"

def test_estructura(db_connection):
    sql = '''
        SELECT tablename FROM pg_tables
        WHERE tablename
        IN ('alumno', 'maestro', 'grupo', 'inscripcion', 'asistencia', 'carrera');
    '''
    expected_tables = {'alumno', 'maestro', 'grupo', 'inscripcion', 'asistencia', 'carrera'}
    with db_connection.cursor() as cur:
        cur.execute(sql)
        result_tables = {row[0] for row in cur.fetchall()}
        msg = f"Tablas esperadas {expected_tables}, pero se encontraron {result_tables}"
        assert result_tables == expected_tables, msg
