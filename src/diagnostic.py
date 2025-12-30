"""
Diagnóstico Completo do Pipeline de Dados
==========================================
Verifica se tudo está pronto para execução.
"""

import subprocess
import sys
from pathlib import Path
import psycopg2


def check_emoji(condition):
    return "✅" if condition else "❌"


def print_section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print('='*60)


def check_docker():
    """Verifica se Docker está rodando."""
    print_section("🐳 VERIFICANDO DOCKER")
    
    try:
        result = subprocess.run(
            ['docker', 'ps', '--format', '{{.Names}}'],
            capture_output=True,
            text=True,
            timeout=5
        )
        
        containers = result.stdout.strip().split('\n')
        containers = [c for c in containers if c]  # Remove vazios
        
        if containers:
            print(f"{check_emoji(True)} Docker está rodando")
            print(f"   Containers ativos: {len(containers)}")
            for container in containers:
                print(f"   • {container}")
            return True
        else:
            print(f"{check_emoji(False)} Docker rodando mas SEM containers")
            print("   💡 Inicie o container PostgreSQL primeiro!")
            return False
            
    except FileNotFoundError:
        print(f"{check_emoji(False)} Docker não encontrado")
        print("   💡 Instale o Docker Desktop")
        return False
    except subprocess.TimeoutExpired:
        print(f"{check_emoji(False)} Docker não responde")
        print("   💡 Reinicie o Docker Desktop")
        return False
    except Exception as e:
        print(f"{check_emoji(False)} Erro: {e}")
        return False


def check_postgres_connection():
    """Verifica conexão com PostgreSQL."""
    print_section("🔌 VERIFICANDO CONEXÃO POSTGRESQL")
    
    try:
        conn = psycopg2.connect(
            host='localhost',
            port=5432,
            database='ecommerce',
            user='postgres',
            password='postgres',
            connect_timeout=3
        )
        
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        
        print(f"{check_emoji(True)} Conexão estabelecida")
        print(f"   PostgreSQL: {version.split(',')[0]}")
        
        cursor.close()
        conn.close()
        return True
        
    except psycopg2.OperationalError as e:
        print(f"{check_emoji(False)} Não foi possível conectar")
        print(f"   Erro: {e}")
        print("   💡 Verifique se o container PostgreSQL está rodando")
        print("   💡 Comando: docker ps")
        return False
    except Exception as e:
        print(f"{check_emoji(False)} Erro: {e}")
        return False


def check_database_tables():
    """Verifica se as tabelas existem."""
    print_section("📊 VERIFICANDO TABELAS NO BANCO")
    
    try:
        conn = psycopg2.connect(
            host='localhost',
            port=5432,
            database='ecommerce',
            user='postgres',
            password='postgres'
        )
        
        cursor = conn.cursor()
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public';
        """)
        
        tables = [row[0] for row in cursor.fetchall()]
        
        required_tables = ['customers', 'orders']
        all_exist = all(table in tables for table in required_tables)
        
        print(f"{check_emoji(all_exist)} Tabelas encontradas: {len(tables)}")
        
        for table in required_tables:
            exists = table in tables
            print(f"   {check_emoji(exists)} {table}")
            
            if exists:
                cursor.execute(f"SELECT COUNT(*) FROM {table};")
                count = cursor.fetchone()[0]
                print(f"      Registros: {count:,}")
        
        if not all_exist:
            print("\n   💡 Execute o script SQL de criação das tabelas:")
            print("   💡 docker exec -i <container> psql -U postgres -d ecommerce < schema.sql")
        
        cursor.close()
        conn.close()
        return all_exist
        
    except Exception as e:
        print(f"{check_emoji(False)} Erro ao verificar tabelas: {e}")
        return False


def check_data_files():
    """Verifica arquivos de dados."""
    print_section("📁 VERIFICANDO ARQUIVOS DE DADOS")
    
    layers = {
        'Bronze': Path('data/bronze'),
        'Silver': Path('data/silver'),
        'Rejected': Path('data/rejected')
    }
    
    all_ok = True
    
    for layer_name, layer_path in layers.items():
        exists = layer_path.exists()
        print(f"\n{check_emoji(exists)} {layer_name}: {layer_path}")
        
        if exists:
            files = list(layer_path.glob('*.parquet'))
            print(f"   Arquivos .parquet: {len(files)}")
            for file in files:
                size_mb = file.stat().st_size / (1024 * 1024)
                print(f"   • {file.name} ({size_mb:.2f} MB)")
        else:
            if layer_name in ['Bronze', 'Silver']:
                all_ok = False
                print(f"   ⚠️  Pasta não encontrada!")
    
    return all_ok


def check_python_dependencies():
    """Verifica dependências Python."""
    print_section("🐍 VERIFICANDO DEPENDÊNCIAS PYTHON")
    
    required = [
        'pandas',
        'sqlalchemy',
        'psycopg2',
        'pyarrow'
    ]
    
    all_installed = True
    
    for package in required:
        try:
            __import__(package)
            print(f"{check_emoji(True)} {package}")
        except ImportError:
            print(f"{check_emoji(False)} {package} - NÃO INSTALADO")
            all_installed = False
    
    if not all_installed:
        print("\n   💡 Instale as dependências:")
        print("   💡 pip install pandas sqlalchemy psycopg2-binary pyarrow")
    
    return all_installed


def main():
    """Executa diagnóstico completo."""
    print("\n" + "="*60)
    print("  🔍 DIAGNÓSTICO DO PIPELINE DE DADOS")
    print("="*60)
    
    results = {
        'Docker': check_docker(),
        'PostgreSQL': check_postgres_connection(),
        'Tabelas': check_database_tables(),
        'Arquivos': check_data_files(),
        'Python': check_python_dependencies()
    }
    
    # Resumo Final
    print_section("📋 RESUMO")
    
    for component, status in results.items():
        print(f"{check_emoji(status)} {component}")
    
    all_ok = all(results.values())
    
    print("\n" + "="*60)
    if all_ok:
        print("  ✅ TUDO PRONTO! Você pode executar o pipeline.")
    else:
        print("  ⚠️  ATENÇÃO! Corrija os problemas acima antes de continuar.")
    print("="*60)
    
    # Próximos passos
    print("\n📝 PRÓXIMOS PASSOS:")
    
    if not results['Docker']:
        print("   1. Inicie o Docker Desktop")
        print("   2. Execute: docker-compose up -d")
    
    if results['Docker'] and not results['PostgreSQL']:
        print("   1. Verifique se o container PostgreSQL está rodando: docker ps")
        print("   2. Se não estiver, inicie: docker-compose up -d")
    
    if results['PostgreSQL'] and not results['Tabelas']:
        print("   1. Execute o script SQL de criação das tabelas")
        print("   2. Comando: docker exec -i <container_id> psql -U postgres -d ecommerce < schema.sql")
    
    if not results['Arquivos']:
        print("   1. Execute bronze_ingestion.py primeiro")
        print("   2. Execute silver_transformation.py")
    
    if results['Arquivos'] and results['Tabelas']:
        print("   1. Execute: python silver_transformation.py")
        print("   2. Execute: python gold_loader.py")
    
    print("="*60)
    
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())