"""
Pipeline Monitoring and Reporting
"""
import json
import logging
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent
LOG_DIR = PROJECT_ROOT / "logs"
REPORT_DIR = PROJECT_ROOT / "data" / "processed"

class PipelineMonitor:
    def __init__(self):
        self.report_file = REPORT_DIR / f"pipeline_execution_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    def generate_report(self, orchestrator):
        """Generate execution report from orchestrator"""
        report = {
            "execution_date": datetime.now().isoformat(),
            "status": orchestrator.execution_report.get('status', 'UNKNOWN'),
            "phases": orchestrator.execution_report.get('phases', {}),
            "errors": orchestrator.execution_report.get('errors', []),
            "total_duration_seconds": sum(
                phase.get('duration_seconds', 0) 
                for phase in orchestrator.execution_report.get('phases', {}).values()
            )
        }
        
        return report
    
    def save_report(self, report):
        """Save report to JSON file"""
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        with open(self.report_file, 'w') as f:
            json.dump(report, f, indent=2)
        print(f"Report saved to: {self.report_file}")
        return self.report_file
    
    def print_summary(self, report):
        """Print execution summary"""
        print("\n" + "="*80)
        print("PIPELINE EXECUTION REPORT")
        print("="*80)
        print(f"Execution Date: {report['execution_date']}")
        print(f"Status: {report['status']}")
        print(f"Total Duration: {report['total_duration_seconds']:.2f} seconds")
        print(f"\nPhases Executed:")
        for phase, details in report['phases'].items():
            status = details.get('status', 'UNKNOWN')
            duration = details.get('duration_seconds', 0)
            print(f"  ✓ {phase}: {status} ({duration:.2f}s)")
        
        if report['errors']:
            print(f"\nErrors ({len(report['errors'])}):")
            for error in report['errors']:
                print(f"  ✗ {error['phase']}: {error['error']}")
        print("="*80 + "\n")
