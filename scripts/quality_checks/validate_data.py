"""
Data Quality Checks Module
Validates data across 5+ quality dimensions
"""

import psycopg2
import json
import logging
from datetime import datetime
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database connection details
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'ecommerce_db'
DB_USER = 'admin'
DB_PASSWORD = 'password'


class DataQualityChecker:
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.quality_report = {
            'check_timestamp': datetime.now().isoformat(),
            'checks_performed': {},
            'overall_quality_score': 100,
            'quality_grade': 'A'
        }
    
    def connect(self):
        """Connect to database"""
        try:
            self.conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            self.cursor = self.conn.cursor()
            logger.info(f"✓ Connected to database")
            return True
        except psycopg2.Error as e:
            logger.error(f"✗ Connection failed: {str(e)}")
            return False
    
    # ============================================================
    # CHECK 1: COMPLETENESS - Check for NULL values
    # ============================================================
    def check_null_values(self):
        """Check for NULL values in mandatory fields"""
        logger.info("Checking NULL values...")
        
        checks = {
            'staging.customers': ['customer_id', 'email', 'first_name', 'last_name'],
            'staging.products': ['product_id', 'product_name', 'price', 'cost'],
            'staging.transactions': ['transaction_id', 'customer_id', 'transaction_date'],
            'staging.transaction_items': ['item_id', 'transaction_id', 'product_id', 'quantity', 'line_total']
        }
        
        null_violations = {}
        total_nulls = 0
        
        for table, columns in checks.items():
            for column in columns:
                query = f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL"
                self.cursor.execute(query)
                count = self.cursor.fetchone()[0]
                
                if count > 0:
                    if table not in null_violations:
                        null_violations[table] = {}
                    null_violations[table][column] = count
                    total_nulls += count
        
        status = 'passed' if total_nulls == 0 else 'failed'
        logger.info(f"  NULL values found: {total_nulls}")
        
        return {
            'status': status,
            'null_violations': total_nulls,
            'details': null_violations
        }
    
    # ============================================================
    # CHECK 2: UNIQUENESS - Check for duplicate IDs
    # ============================================================
    def check_duplicates(self):
        """Check for duplicate IDs"""
        logger.info("Checking for duplicates...")
        
        id_columns = {
            'staging.customers': 'customer_id',
            'staging.products': 'product_id',
            'staging.transactions': 'transaction_id',
            'staging.transaction_items': 'item_id'
        }
        
        duplicates = {}
        total_duplicates = 0
        
        for table, id_column in id_columns.items():
            query = f"""
            SELECT {id_column}, COUNT(*) as cnt
            FROM {table}
            GROUP BY {id_column}
            HAVING COUNT(*) > 1
            """
            self.cursor.execute(query)
            results = self.cursor.fetchall()
            
            if results:
                duplicates[table] = len(results)
                total_duplicates += len(results)
        
        status = 'passed' if total_duplicates == 0 else 'failed'
        logger.info(f"  Duplicate IDs found: {total_duplicates}")
        
        return {
            'status': status,
            'duplicates_found': total_duplicates,
            'details': duplicates
        }
    
    # ============================================================
    # CHECK 3: VALIDITY - Check data formats and ranges
    # ============================================================
    def check_data_validity(self):
        """Check data validity (formats, ranges, etc.)"""
        logger.info("Checking data validity...")
        
        violations = {}
        total_violations = 0
        
        # Check 1: Price > 0
        query = "SELECT COUNT(*) FROM staging.products WHERE price <= 0"
        self.cursor.execute(query)
        count = self.cursor.fetchone()[0]
        if count > 0:
            violations['negative_price'] = count
            total_violations += count
        
        # Check 2: Cost > 0
        query = "SELECT COUNT(*) FROM staging.products WHERE cost <= 0"
        self.cursor.execute(query)
        count = self.cursor.fetchone()[0]
        if count > 0:
            violations['negative_cost'] = count
            total_violations += count
        
        # Check 3: Quantity > 0
        query = "SELECT COUNT(*) FROM staging.transaction_items WHERE quantity <= 0"
        self.cursor.execute(query)
        count = self.cursor.fetchone()[0]
        if count > 0:
            violations['invalid_quantity'] = count
            total_violations += count
        
        # Check 4: Discount 0-100%
        query = "SELECT COUNT(*) FROM staging.transaction_items WHERE discount_percentage < 0 OR discount_percentage > 100"
        self.cursor.execute(query)
        count = self.cursor.fetchone()[0]
        if count > 0:
            violations['invalid_discount'] = count
            total_violations += count
        
        status = 'passed' if total_violations == 0 else 'failed'
        logger.info(f"  Validity violations: {total_violations}")
        
        return {
            'status': status,
            'violations': total_violations,
            'details': violations
        }
    
    # ============================================================
    # CHECK 4: CONSISTENCY - Check calculations
    # ============================================================
    def check_consistency(self):
        """Check data consistency (calculations)"""
        logger.info("Checking data consistency...")
        
        violations = {}
        total_violations = 0
        
        # Check: line_total = quantity * unit_price * (1 - discount_percentage/100)
        query = """
        SELECT COUNT(*) FROM staging.transaction_items
        WHERE line_total != ROUND(quantity * unit_price * (1 - discount_percentage/100.0), 2)
        """
        self.cursor.execute(query)
        count = self.cursor.fetchone()[0]
        if count > 0:
            violations['line_total_mismatch'] = count
            total_violations += count
        
        status = 'passed' if total_violations == 0 else 'failed'
        logger.info(f"  Consistency violations: {total_violations}")
        
        return {
            'status': status,
            'violations': total_violations,
            'details': violations
        }
    
    # ============================================================
    # CHECK 5: REFERENTIAL INTEGRITY - Check foreign keys
    # ============================================================
    def check_referential_integrity(self):
        """Check referential integrity (no orphan records)"""
        logger.info("Checking referential integrity...")
        
        orphans = {}
        total_orphans = 0
        
        # Check 1: Transactions with invalid customer_id
        query = """
        SELECT COUNT(*) FROM staging.transactions t
        WHERE NOT EXISTS (
            SELECT 1 FROM staging.customers c WHERE c.customer_id = t.customer_id
        )
        """
        self.cursor.execute(query)
        count = self.cursor.fetchone()[0]
        if count > 0:
            orphans['invalid_customer_in_transactions'] = count
            total_orphans += count
        
        # Check 2: Items with invalid transaction_id
        query = """
        SELECT COUNT(*) FROM staging.transaction_items ti
        WHERE NOT EXISTS (
            SELECT 1 FROM staging.transactions t WHERE t.transaction_id = ti.transaction_id
        )
        """
        self.cursor.execute(query)
        count = self.cursor.fetchone()[0]
        if count > 0:
            orphans['invalid_transaction_in_items'] = count
            total_orphans += count
        
        # Check 3: Items with invalid product_id
        query = """
        SELECT COUNT(*) FROM staging.transaction_items ti
        WHERE NOT EXISTS (
            SELECT 1 FROM staging.products p WHERE p.product_id = ti.product_id
        )
        """
        self.cursor.execute(query)
        count = self.cursor.fetchone()[0]
        if count > 0:
            orphans['invalid_product_in_items'] = count
            total_orphans += count
        
        status = 'passed' if total_orphans == 0 else 'failed'
        logger.info(f"  Orphan records found: {total_orphans}")
        
        return {
            'status': status,
            'orphan_records': total_orphans,
            'details': orphans
        }
    
    # ============================================================
    # CALCULATE OVERALL QUALITY SCORE
    # ============================================================
    def calculate_quality_score(self, all_checks):
        """Calculate overall quality score (0-100)"""
        
        total_issues = 0
        
        # Count all violations
        for check_name, check_result in all_checks.items():
            if 'violations' in check_result:
                total_issues += check_result['violations']
            if 'null_violations' in check_result:
                total_issues += check_result['null_violations']
            if 'duplicates_found' in check_result:
                total_issues += check_result['duplicates_found']
            if 'orphan_records' in check_result:
                total_issues += check_result['orphan_records']
        
        # Calculate score (deduct for each issue)
        # Get total record count
        self.cursor.execute("SELECT COUNT(*) FROM staging.customers")
        customer_count = self.cursor.fetchone()[0]
        self.cursor.execute("SELECT COUNT(*) FROM staging.products")
        product_count = self.cursor.fetchone()[0]
        self.cursor.execute("SELECT COUNT(*) FROM staging.transactions")
        transaction_count = self.cursor.fetchone()[0]
        self.cursor.execute("SELECT COUNT(*) FROM staging.transaction_items")
        item_count = self.cursor.fetchone()[0]
        
        total_records = customer_count + product_count + transaction_count + item_count
        
        if total_issues == 0:
            quality_score = 100
        else:
            quality_score = max(0, 100 - (total_issues / total_records * 100))
        
        # Assign grade
        if quality_score >= 95:
            grade = 'A'
        elif quality_score >= 85:
            grade = 'B'
        elif quality_score >= 75:
            grade = 'C'
        elif quality_score >= 60:
            grade = 'D'
        else:
            grade = 'F'
        
        return round(quality_score, 2), grade
    
    def run_all_checks(self):
        """Run all quality checks"""
        logger.info("=" * 60)
        logger.info("Starting Data Quality Checks")
        logger.info("=" * 60)
        
        if not self.connect():
            return None
        
        try:
            # Run all checks
            self.quality_report['checks_performed'] = {
                'null_checks': self.check_null_values(),
                'duplicate_checks': self.check_duplicates(),
                'validity_checks': self.check_data_validity(),
                'consistency_checks': self.check_consistency(),
                'referential_integrity': self.check_referential_integrity()
            }
            
            # Calculate overall score
            quality_score, grade = self.calculate_quality_score(
                self.quality_report['checks_performed']
            )
            
            self.quality_report['overall_quality_score'] = quality_score
            self.quality_report['quality_grade'] = grade
            
            logger.info("=" * 60)
            logger.info(f"✓ Quality Checks Complete!")
            logger.info(f"  Overall Score: {quality_score}/100")
            logger.info(f"  Grade: {grade}")
            logger.info("=" * 60)
            
            return self.quality_report
            
        except Exception as e:
            logger.error(f"Error during quality checks: {str(e)}")
            return None
        
        finally:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
    
    def save_quality_report(self, output_dir='data/staging'):
        """Save quality report to JSON"""
        try:
            Path(output_dir).mkdir(parents=True, exist_ok=True)
            
            report_file = f'{output_dir}/quality_report.json'
            with open(report_file, 'w') as f:
                json.dump(self.quality_report, f, indent=2)
            
            logger.info(f"✓ Quality report saved to {report_file}")
        except Exception as e:
            logger.error(f"Failed to save report: {str(e)}")


def main():
    """Main execution"""
    checker = DataQualityChecker()
    report = checker.run_all_checks()
    checker.save_quality_report()
    return report


if __name__ == '__main__':
    main()
