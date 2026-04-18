import os
import sys
from pathlib import Path
from soda.scan import Scan

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

import config

logger = config.get_logger(__name__)

def run_soda_scan(scan_name: str, checks_file: str):
    """
    Runs a Soda scan for a specific checks file.
    """
    logger.info(f"Starting Soda scan: {scan_name} using {checks_file}...")
    
    soda_dir = PROJECT_ROOT / "data_quality"
    config_file = soda_dir / "soda_config.yml"
    checks_path = soda_dir / checks_file
    
    if not config_file.exists():
        logger.error(f"Soda config not found: {config_file}")
        return False
        
    if not checks_path.exists():
        logger.error(f"Soda checks file not found: {checks_path}")
        return False

    scan = Scan()
    scan.set_data_source_name(scan_name)
    scan.add_configuration_yaml_file(str(config_file))
    scan.add_sodacl_yaml_file(str(checks_path))
    
    # Run the scan
    result = scan.execute()
    
    # Analyze results
    if result != 0:
        logger.warning(f"Soda scan '{scan_name}' completed with failures/warnings. (Code: {result})")
        # scan.get_all_checks_text() can be used for deep logging if needed
    else:
        logger.info(f"Soda scan '{scan_name}' completed SUCCESSFULLY.")
        
    return result == 0

def run_all_soda_checks():
    """ Runs both transactions and stocks checks. """
    t_success = run_soda_scan("transactions_silver", "checks_transactions.yml")
    s_success = run_soda_scan("stocks_silver", "checks_stocks.yml")
    return t_success and s_success

if __name__ == "__main__":
    run_all_soda_checks()
