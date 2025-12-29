"""
INSPEÇÃO COMPLETA DE TODOS OS ARQUIVOS JSON
============================================
Verifica quais arquivos têm relação com customers (email/customer_id).
"""

import json
from pathlib import Path


JSON_DIR = Path('data') / 'raw' / 'json_files'


def inspect_json_for_customer_relation(filename: str):
    """Verifica se JSON tem campos que relacionam com customers."""
    filepath = JSON_DIR / filename
    
    print(f"\n{'='*70}")
    print(f"📄 {filename}")
    print(f"{'='*70}")
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if isinstance(data, list) and len(data) > 0:
            first_record = data[0]
            
            print(f"Total de registros: {len(data)}")
            print(f"\nColunas ({len(first_record)}):")
            
            # Identifica colunas relacionadas a customer
            customer_related = []
            email_related = []
            
            for col in first_record.keys():
                col_lower = col.lower()
                
                # Marca colunas importantes
                if 'customer' in col_lower:
                    customer_related.append(col)
                    print(f"  ✓ '{col}' ← RELACIONADO A CUSTOMER")
                elif 'email' in col_lower:
                    email_related.append(col)
                    print(f"  ✓ '{col}' ← CONTÉM EMAIL")
                elif 'user' in col_lower or 'client' in col_lower:
                    customer_related.append(col)
                    print(f"  ✓ '{col}' ← PODE SER CUSTOMER")
                else:
                    print(f"    '{col}'")
            
            # Resumo
            if customer_related or email_related:
                print(f"\n🎯 POTENCIAL PARA ORDERS:")
                print(f"   Campos de customer: {customer_related}")
                print(f"   Campos de email: {email_related}")
                
                # Mostra exemplo dos campos importantes
                if email_related:
                    print(f"\n   Exemplo de valores:")
                    for col in email_related:
                        value = first_record.get(col)
                        print(f"   {col}: {value}")
            else:
                print(f"\n❌ NÃO TEM campos relacionados a customer")
        
        else:
            print("ERRO: Arquivo vazio ou formato inesperado")
    
    except FileNotFoundError:
        print(f"ERRO: Arquivo não encontrado")
    except Exception as e:
        print(f"ERRO: {e}")


def main():
    """Inspeciona todos os arquivos JSON."""
    print("\n" + "="*70)
    print("🔍 BUSCA POR ARQUIVOS QUE PODEM SER 'ORDERS'")
    print("="*70)
    print("Procurando por campos: customer_id, customer_email, email, user_id...")
    
    # Lista de arquivos para verificar
    json_files = [
        'customers_master.json',
        'payment_transactions.json',
        'customer_reviews_jan.json',
        'customer_reviews_feb.json',
        'customer_support_tickets.json',
        'email_marketing_sends.json',
        'marketing_campaigns_q1.json',
        'inventory_adjustments_jan.json',
        'inventory_adjustments_feb.json',
        'competitor_pricing.json'
    ]
    
    for json_file in json_files:
        inspect_json_for_customer_relation(json_file)
    
    print("\n" + "="*70)
    print("🎯 RECOMENDAÇÃO:")
    print("="*70)
    print("Use o arquivo que tiver:")
    print("  1. customer_id ou customer_email (MELHOR)")
    print("  2. email relacionado ao cliente")
    print("  3. Valores monetários (amount, total, price)")
    print("  4. Datas de compra/transação")
    print("="*70 + "\n")


if __name__ == "__main__":
    main()