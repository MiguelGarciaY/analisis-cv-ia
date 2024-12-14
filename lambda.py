import json
from datetime import datetime
import openai
import boto3
import os
import base64
from io import BytesIO
import pytz
import logging
from PyPDF2 import PdfReader
import re
import uuid
from decimal import Decimal

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
bucket_name = os.getenv("BUCKET")
table_name = os.getenv("TABLE_NAME")
table_name_analisis = os.getenv("TABLE_NAME_ANALISIS")
openai.api_key = os.getenv("OPENAI_API_KEY")
#dynamodb = boto3.client('dynamodb', region_name='us-east-1')
class AnalisisIa:
    def __init__(self):
        self.now_date = str(datetime.strftime(datetime.now(pytz.timezone('America/Lima')), "%Y-%m-%d %H:%M:%S"))
        self.str_date = str(datetime.strftime(datetime.now(pytz.timezone('America/Lima')), "%Y%m%d%H%M%S%f"))
        self.year_date = str(datetime.strftime(datetime.now(pytz.timezone('America/Lima')), "%Y"))

    def myconverter(self, o):
        if isinstance(o, datetime):
            return o.__str__()

    def analyze_cv_handler(self, body, context):
        try:
            print("VERSION OPENAI")
            print(openai.__version__)
            pdf_content = self.get_text_pdf(body)
            
            requisitos = body.get('requisitos')
            tareas = body.get('tareas')
            dni = body.get('dni')
            name = body.get('name')

            if not pdf_content or not requisitos or not tareas:
                return {
                    "statusCode": 400,
                    "body": json.dumps({"error": "Missing required fields: 'pdf_content', 'requisitos', or 'tareas'."})
                }

            # Analyze CV using OpenAI
            prompt = (
                f"Analiza el siguiente curriculum vitae (CV) para una solicitud de empleo. "
                f"La posición requiere calificaciones específicas y tareas detalladas a continuación. "
                f"Proporciona una evaluación estructurada en formato JSON en español, incluyendo:\n\n"
                f"1. Una puntuación general de idoneidad (sobre 100) basada en la relevancia para el puesto.\n"
                f"2. Una lista de las principales fortalezas del candidato.\n"
                f"3. Una lista de debilidades o áreas que no se alinean con los requisitos del puesto.\n\n"
                f"Requisitos del Puesto:\n{', '.join(requisitos)}\n\n"
                f"Tareas del Puesto:\n{', '.join(tareas)}\n\n"
                f"Contenido del CV:\n{pdf_content}\n\n"
                f"Devuelve el análisis en el siguiente formato JSON:\n"
                f"{{\n"
                f"  \"puntuación_idoneidad\": <número>,\n"
                f"  \"fortalezas_clave\": [\"<fortaleza_1>\", \"<fortaleza_2>\", ...],\n"
                f"  \"debilidades\": [\"<debilidad_1>\", \"<debilidad_2>\", ...]\n"
                f"}}"
            )

            response = openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "You are an AI expert analyzing CVs."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=1500
            )

            result = response['choices'][0]['message']['content']

            analysis_id=0
            try:
                # Busca un bloque JSON usando una expresión regular
                json_match = re.search(r"{.*}", result, re.DOTALL)
                if json_match:
                    # Extraer el JSON válido
                    json_string = json_match.group(0)
                    analysis_data = json.loads(json_string)
                    print(analysis_data)
                    id = str(uuid.uuid4())

                    table = dynamodb.Table(table_name_analisis)

                    fortalezas = analysis_data.get('fortalezas_clave', [])
                    debilidades = analysis_data.get('debilidades', [])
                    puntaje = analysis_data.get('puntuación_idoneidad', 0)
                    item = {
                        'id': id,
                        'dni': dni,
                        'name': name,
                        'fortalezas': fortalezas,
                        'debilidades': debilidades,
                        'puntaje': puntaje
                    }
                    table.put_item(Item=item)
                else:
                    # Si no se encuentra JSON, envuelve el contenido como un string
                    analysis_data = {
                        "analysis_id": analysis_id,
                        "analysis": result
                    }
            except json.JSONDecodeError:
                # Si ocurre un error al intentar cargar el JSON
                analysis_data = {
                    "analysis_id": analysis_id,
                    "analysis": result
                }
            

            return {
                "statusCode": 200,
                "body": analysis_data
            }

        except Exception as e:
            return {
                "statusCode": 500,
                "body": json.dumps({"error": str(e)})
            }

    def compare_cvs_handler(self):
        try:
            # Fetch all analyses from DynamoDB
            table = dynamodb.Table(table_name)

            response = table.scan()
            items = response.get('Items', [])

            if not items:
                return {
                    "statusCode": 404,
                    "body": json.dumps({"error": "No analyses found."})
                }

            # Compare CVs and find the best one
            best_cv = max(items, key=lambda item: item.get('score', 0))

            return {
                "statusCode": 200,
                "body": json.dumps({"best_cv": best_cv})
            }

        except Exception as e:
            return {
                "statusCode": 500,
                "body": json.dumps({"error": str(e)})
            }

    def upload_document(self, body):
        logger.info('Enter to upload_document')
        print(openai.__version__)
        get_file_content = body['file']
        filename = body['filename']
        dni = body['dni']
        user_id = body['user_id']
        try:
            S3 = boto3.client( 's3' )
            decode_content = base64.b64decode(get_file_content)
            year = datetime.now().year
            
            file_name, file_extension  = os.path.splitext(filename)
            file_extension = file_extension[1:]
            path = f'analisis-ia/{year}/{user_id}/{filename}'

            s3_upload = S3.put_object(
                Bucket= bucket_name,
                Key= path,
                Body=decode_content
            )

            rs = { 
                'status_code' : 200 , 
                'mesagge' : 'File uploaded successfully',
                'path': path
            }
        except Exception as e:
            logger.error(f"Error en el metodo upload_document: {e}")
            return 500, {"response": f"Error procesando el archivo: {e}"}
        return 200, rs

    def get_text_pdf(self, body):
        texto = ''
        pdf = self.download_document_memory(body)
        if pdf is not None:
            texto = self.extract_text_from_pdf_in_memory(pdf)
        return 200, texto

    def download_document_memory(self, body):
        s3 = boto3.client("s3")
        s3_key = body['ruta_pdf']
        try:
            response = s3.get_object(Bucket=bucket_name, Key=s3_key)
            pdf_content = BytesIO(response['Body'].read())  # Cargar contenido en memoria
            print("Archivo descargado exitosamente en memoria.")
            return pdf_content
        except Exception as e:
            print(f"Error descargando el archivo desde S3: {e}")
            return None

    def extract_text_from_pdf_in_memory(self, pdf_content):
        texto = ''
        try:
            reader = PdfReader(pdf_content)
            for page_number, page in enumerate(reader.pages, start=1):
                page_text = page.extract_text()
                page_text = ' '.join(page_text.split())
                texto += page_text + ' '
            return texto.strip()
        except Exception as e:
            print(f"Error extrayendo texto del PDF: {e}")
            return texto

    def create_user(self, body):
        print("Create_user")
        email = body.get('email')
        name = body.get('name')
        last_name = body.get('last_name')
        age = body.get('age')
        date_of_birth = body.get('date_of_birth')
        pdf_file_base64 = body.get('file')
        filename = body.get('filename')
        dni = body.get('dni')
        role = body.get('role')
        password = body.get('password')
        print("1")
        if not all([dni, name, last_name, date_of_birth, pdf_file_base64]):
            return 400, {
                'statusCode': 400,
                'body': json.dumps({'message': 'Faltan campos requeridos.'})
            }
        
        user_id = str(uuid.uuid4())
        body['user_id'] = user_id
        print("2")
        try:
            pdf_bytes = base64.b64decode(pdf_file_base64)
            rsp_up_document, response_up_document = self.upload_document(body)
        except base64.binascii.Error as e:
            return 400, {
                'statusCode': 400,
                'body': json.dumps({'message': 'El archivo PDF no está en formato Base64 válido.'})
            }
        print("3")
        #user_id = str(uuid.uuid4())
        table = dynamodb.Table(table_name)
        print("4")
        item = {
            'userId': user_id,
            'email': email,
            'name': name,
            'last_name': last_name,
            #'age': int(age),
            'date_of_birth': date_of_birth,
            'pdf_file_path': response_up_document.get('path'),
            'created_at': datetime.utcnow().isoformat(),
            'role': role,
            'password': password,
            'dni': dni
        }
        print("5")
        table.put_item(Item=item)

        rsp = {
            'statusCode': 201,
            'body': json.dumps({'message': 'Usuario creado exitosamente.', 'userId': user_id})
        }
        return 200, rsp

    def get_user(self, body):
        try:
            print("GET USERR")
            table = dynamodb.Table(table_name)
            response = table.scan()
            items = response.get('Items', [])
            converted_items = self.decimal_to_native(items)
            rs = {
                'statusCode': 200,
                'body': {'data': converted_items}
            }

        except Exception as e:
            logger.error(f"Error interno del servidor: {e}")
            return 500, {"response": f"Error interno del servidor: {e}"}
        return 200, rs

    def get_analisis_ia(self, body):
        try:
            print("GET ANALISIS IA")
            table = dynamodb.Table(table_name_analisis)
            response = table.scan()
            items = response.get('Items', [])
            converted_items = self.decimal_to_native(items)
            rs = {
                'statusCode': 200,
                'body': {'data': converted_items}
            }

        except Exception as e:
            logger.error(f"Error interno del servidor: {e}")
            return 500, {"response": f"Error interno del servidor: {e}"}
        return 200, rs

    def decimal_to_native(self, obj):
        """
        Convierte objetos de tipo Decimal a tipos nativos de Python.
        """
        if isinstance(obj, list):
            return [self.decimal_to_native(i) for i in obj]
        elif isinstance(obj, dict):
            return {k: self.decimal_to_native(v) for k, v in obj.items()}
        elif isinstance(obj, Decimal):
            # Convertir Decimal a int o float según sea necesario
            return int(obj) if obj % 1 == 0 else float(obj)
        else:
            return obj


def lambda_handler(event, context):
    logger.info("Event received: %s", event)

    body = json.loads(event['body']) if event.get('body') else {}

    statusCode = 400
    rsp = {"message": "warning1"}

    rl = AnalisisIa()
    logger.info("Parsed body: %s", body)

    action = body.get('action')
    
    if action == 'analyze_cv':
        rsp = rl.analyze_cv_handler(body, context)
        statusCode = rsp["statusCode"]
    elif action == 'compare_cvs':
        rsp = rl.compare_cvs_handler()
        statusCode = rsp["statusCode"]
    elif action == 'upload-document':
        statusCode, rsp = rl.upload_document(body)
    elif action == 'get-text-pdf':
        statusCode, rsp = rl.get_text_pdf(body)
    elif action == 'create-user':
        statusCode, rsp = rl.create_user(body)
    elif action == 'get-user':
        statusCode, rsp = rl.get_user(body)
    elif action == "get-analisis-ia":
        statusCode, rsp = rl.get_analisis_ia(body)
    elif action == 'otro':
        statusCode = 200
        rsp = {"message": "Exito"}
    else:
        rsp = {"error": "Invalid action specified."}

    return {
        "statusCode": statusCode,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Allow-Methods": "*"
        },
        "body": json.dumps(rsp, default=rl.myconverter)
    }
