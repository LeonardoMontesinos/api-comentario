# -----------------------------
# Archivo: comentario.py
# -----------------------------
import json
import boto3
import os       # Para leer las variables de entorno
import uuid     # Para generar IDs únicos
import logging

# Configurar logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Inicializar clientes fuera del handler (buena práctica)
s3_client = boto3.client('s3')
dynamodb_resource = boto3.resource('dynamodb')

# Obtener nombres de las variables de entorno
TABLE_NAME = os.environ.get('TABLE_NAME')
BUCKET_INGESTA_NAME = os.environ.get('BUCKET_INGESTA_NAME')

def lambda_handler(event, context):
    
    try:
        # 1. Obtener el body (el comentario que envía el usuario)
        # Usamos event.get('body') porque estamos en modo proxy
        raw_body = event.get('body')
        if not raw_body:
            return {'statusCode': 400, 'body': json.dumps('No se recibió body.')}
            
        data = json.loads(raw_body)
        
        # Asumimos que el JSON trae un 'tenant_id' y un 'comentario'
        tenant_id = data.get('tenant_id')
        comentario_texto = data.get('comentario')
        
        if not tenant_id or not comentario_texto:
            return {'statusCode': 400, 'body': json.dumps('Faltan "tenant_id" o "comentario".')}

        # 2. Generar un ID único para este comentario
        # (Como pide el ejercicio 5 sobre UUID)
        comentario_uuid = str(uuid.uuid4())
        
        # ---
        # 3. EJERCICIO 5: ESTRATEGIA "INGESTA PUSH"
        # ---
        # Guardamos el JSON *crudo* en S3 antes de procesarlo.
        # Creamos una "ruta" (Key) para S3, ej: "tenant-123/abc-123.json"
        
        s3_key = f"{tenant_id}/{comentario_uuid}.json"
        
        logger.info(f"Push a S3: Bucket={BUCKET_INGESTA_NAME}, Key={s3_key}")
        
        s3_client.put_object(
            Bucket=BUCKET_INGESTA_NAME,
            Key=s3_key,
            Body=raw_body, # Guardamos el string JSON crudo
            ContentType='application/json'
        )

        # ---
        # 4. LÓGICA DE NEGOCIO (Guardar en DynamoDB)
        # ---
        table = dynamodb_resource.Table(TABLE_NAME)
        
        item = {
            'tenant_id': tenant_id,
            'uuid': comentario_uuid,
            'comentario': comentario_texto
            # ... puedes añadir más atributos como 'fecha_creacion', etc.
        }
        
        logger.info(f"Guardando en DynamoDB: {item}")
        table.put_item(Item=item)
        
        # 5. Enviar respuesta exitosa
        return {
            'statusCode': 201, # 201 = Created
            'headers': {'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({
                'message': 'Comentario creado y registrado en ingesta.',
                'uuid': comentario_uuid,
                's3_path': f"s3://{BUCKET_INGESTA_NAME}/{s3_key}"
            })
        }

    except Exception as e:
        logger.error(f"Error inesperado: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {'Access-Control-Allow-Origin': '*'},
            'body': json.dumps(f'Error interno del servidor: {str(e)}')
        }
