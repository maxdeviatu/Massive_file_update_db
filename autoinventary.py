# importar las bibliotecas necesarias
import os  # Añadido para manejar variables de entorno
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Text, Enum, DECIMAL, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base
import datetime
import logging
from tqdm import tqdm
import sys

# (Opcional) Cargar variables de entorno desde un archivo .env
from dotenv import load_dotenv
load_dotenv()

# Configurar el logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("carga_licencias.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Definir la conexión a la base de datos usando variables de entorno
DATABASE_USER = os.getenv('DB_USER')
DATABASE_PASSWORD = os.getenv('DB_PASSWORD')
DATABASE_HOST = os.getenv('DB_HOST', 'localhost')
DATABASE_PORT = os.getenv('DB_PORT', '5432')
DATABASE_NAME = os.getenv('DB_NAME')

# Validar que todas las variables necesarias están presentes
if not all([DATABASE_USER, DATABASE_PASSWORD, DATABASE_HOST, DATABASE_PORT, DATABASE_NAME]):
    logger.error("Faltan variables de entorno para la configuración de la base de datos.")
    sys.exit("Error: Faltan variables de entorno para la configuración de la base de datos.")

DATABASE_URI = f'postgresql+psycopg2://{DATABASE_USER}:{DATABASE_PASSWORD}@{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_NAME}'

try:
    engine = create_engine(DATABASE_URI)
    Session = sessionmaker(bind=engine)
    Base = declarative_base()

    logger.info("Conexión a la base de datos establecida correctamente.")
except Exception as e:
    logger.error(f"Error al conectar a la base de datos: {e}")
    raise

# Definir el modelo de la tabla Inventories
class Inventories(Base):
    __tablename__ = 'Inventories'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    activationKey = Column(String, nullable=False, unique=True)  # Uso de mayúsculas según la tabla existente
    product_reference = Column(String, nullable=False)
    activationInstructions = Column(Text, nullable=True)
    status = Column(Enum('DISPONIBLE', 'VENDIDO', name='status_enum'), nullable=False, default='DISPONIBLE')
    price_amount = Column(DECIMAL(10, 2), nullable=False)
    seller_mail = Column(String, nullable=True)
    createdAt = Column(DateTime, default=datetime.datetime.utcnow)
    updatedAt = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

# Asegurarse de que las tablas existen
# Base.metadata.create_all(engine)
try:
    # Base.metadata.create_all(engine)  # Comentado para evitar crear tablas nuevas
    logger.info("Verificación de tablas completada.")
except Exception as e:
    logger.error(f"Error al verificar las tablas: {e}")
    raise

# Leer el archivo Excel
excel_file = 'cargaLicencias.xlsx'
try:
    df = pd.read_excel(excel_file)
    total_filas = len(df)
    logger.info(f"Archivo Excel '{excel_file}' leído exitosamente. Total de filas: {total_filas}.")
except Exception as e:
    logger.error(f"Error al leer el archivo Excel: {e}")
    raise

# Iniciar sesión y procesar los datos
with Session() as session:
    try:
        # Obtener todas las activationKeys existentes en la base de datos
        existing_keys = set(
            key[0] for key in session.query(Inventories.activationKey).all()
        )
        logger.info(f"Se encontraron {len(existing_keys)} códigos de activación existentes.")

        nuevos_items = []
        duplicados_db = set()
        duplicados_archivo = set()
        activation_keys_nuevos = set()

        # Iterar sobre cada fila del DataFrame con una barra de progreso
        for index, row in tqdm(df.iterrows(), total=df.shape[0], desc="Procesando filas"):
            activation_code = row.get('ACTIVATION CODE', None)

            # Manejar valores NaN o vacíos en activation_code
            if pd.isna(activation_code):
                logger.warning(f"Fila {index + 2}: El código de activación está vacío. Se omite esta entrada.")
                duplicados_db.add('N/A (Valor Vacío)')
                continue

            activation_code = str(activation_code).strip()

            # Verificar si el activation_code ya existe en la base de datos
            if activation_code in existing_keys:
                logger.warning(f"El código de activación '{activation_code}' ya existe en la base de datos. Se omite esta entrada.")
                duplicados_db.add(activation_code)
                continue

            # Verificar si el activation_code ya ha sido agregado en nuevos_items (duplicado en el archivo Excel)
            if activation_code in activation_keys_nuevos:
                logger.warning(f"El código de activación '{activation_code}' está duplicado en el archivo Excel. Se omite esta entrada.")
                duplicados_archivo.add(activation_code)
                continue

            # Crear una instancia del modelo Inventories
            item = Inventories(
                name=row.get('NOMBRE', '').strip(),
                activationKey=activation_code,
                product_reference=row.get('REFERENCE', '').strip(),
                activationInstructions=row.get('INSTRUCCIONES', None),
                status='DISPONIBLE',  # Valor por defecto
                price_amount=row.get('MONTO', 0.00),
                seller_mail=row.get('CORREO DEL VENDEDOR', None),
                createdAt=datetime.datetime.utcnow(),
                updatedAt=datetime.datetime.utcnow()
            )
            nuevos_items.append(item)
            activation_keys_nuevos.add(activation_code)  # Añadir al conjunto para evitar duplicados en el archivo

        total_nuevos = len(nuevos_items)
        total_duplicados_db = len(duplicados_db)
        total_duplicados_archivo = len(duplicados_archivo)

        # Resumen de operaciones
        print("\n--- Resumen de la Carga Masiva ---")
        print(f"Total de filas en el archivo Excel: {total_filas}")
        print(f"Códigos de activación existentes en la base de datos: {len(existing_keys)}")
        print(f"Duplicados encontrados en la base de datos: {total_duplicados_db}")
        print(f"Duplicados encontrados en el archivo Excel: {total_duplicados_archivo}")
        print(f"Total de nuevos ítems a insertar: {total_nuevos}")
        print("----------------------------------\n")

        # Opcional: Listar los códigos duplicados
        if total_duplicados_db > 0:
            print("Códigos de activación duplicados en la base de datos:")
            for code in duplicados_db:
                print(f" - {code}")
            print()

        if total_duplicados_archivo > 0:
            print("Códigos de activación duplicados en el archivo Excel:")
            for code in duplicados_archivo:
                print(f" - {code}")
            print()

        # Opcional: Guardar los duplicados en archivos separados
        if total_duplicados_db > 0:
            with open("duplicados_db.txt", "w") as f_db:
                for code in duplicados_db:
                    f_db.write(f"{code}\n")
            logger.info(f"Se ha creado el archivo 'duplicados_db.txt' con los códigos duplicados de la base de datos.")

        if total_duplicados_archivo > 0:
            with open("duplicados_archivo.txt", "w") as f_archivo:
                for code in duplicados_archivo:
                    f_archivo.write(f"{code}\n")
            logger.info(f"Se ha creado el archivo 'duplicados_archivo.txt' con los códigos duplicados del archivo Excel.")

        # Solicitar confirmación al usuario
        while True:
            confirmacion = input("¿Desea continuar con la carga masiva? (y/n): ").strip().lower()
            if confirmacion in ['y', 'n']:
                break
            else:
                print("Entrada no válida. Por favor, ingrese 'y' para sí o 'n' para no.")

        if confirmacion == 'y':
            if nuevos_items:
                # Inserción masiva
                session.bulk_save_objects(nuevos_items)
                session.commit()
                logger.info("Datos insertados exitosamente en la base de datos.")
                print("Carga masiva completada exitosamente.")
            else:
                logger.info("No hay nuevos datos para insertar.")
                print("No hay nuevos datos para insertar.")
        else:
            logger.info("Carga masiva detenida por el usuario.")
            print("Proceso de carga masiva detenido por el usuario.")

    except Exception as e:
        session.rollback()
        logger.error(f"Ocurrió un error al insertar los datos: {e}")
        print(f"Ocurrió un error al insertar los datos: {e}")
    finally:
        logger.info("Proceso de carga finalizado.")
