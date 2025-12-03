import os
import sys
import csv
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def validate_csv_file(csv_path):
    """Validate the output CSV file"""
    logger.info(f"Validating CSV file: {csv_path}")

    if not os.path.exists(csv_path):
        logger.error(f"CSV file not found: {csv_path}")
        return False

    expected_columns = [
        "device_type",
        "event_date",
        "percentile_95",
        "mean_duration",
        "stddev_duration",
        "total_events",
        "distinct_devices"
    ]

    validation_results = {
        "file_exists": True,
        "has_header": False,
        "columns_match": False,
        "has_data": False,
        "data_valid": False,
        "min_events_check": False
    }

    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            # Check header
            if reader.fieldnames:
                validation_results["has_header"] = True
                actual_columns = list(reader.fieldnames)

                # Check columns match
                if set(actual_columns) == set(expected_columns):
                    validation_results["columns_match"] = True
                else:
                    logger.warning(f"Column mismatch. Expected: {expected_columns}, Got: {actual_columns}")

                # Validate data
                row_count = 0
                valid_rows = 0
                min_events_violations = 0

                for row in reader:
                    row_count += 1

                    # Check required fields are present and valid
                    try:
                        device_type = row.get("device_type", "").strip()
                        event_date = row.get("event_date", "").strip()
                        percentile_95 = float(row.get("percentile_95", 0))
                        mean_duration = float(row.get("mean_duration", 0))
                        total_events = int(row.get("total_events", 0))
                        distinct_devices = int(row.get("distinct_devices", 0))

                        # Validate values
                        if (device_type and event_date and
                                percentile_95 > 0 and mean_duration > 0 and
                                total_events > 0 and distinct_devices > 0):
                            valid_rows += 1

                            # Check minimum events requirement (500)
                            if total_events < 500:
                                min_events_violations += 1
                                logger.warning(
                                    f"Row {row_count}: {device_type} on {event_date} has only "
                                    f"{total_events} events (minimum: 500)"
                                )
                        else:
                            logger.warning(f"Row {row_count} has invalid data")

                    except (ValueError, KeyError) as e:
                        logger.warning(f"Row {row_count} validation error: {str(e)}")

                validation_results["has_data"] = row_count > 0
                validation_results["data_valid"] = valid_rows == row_count
                validation_results["min_events_check"] = min_events_violations == 0

                logger.info(f"Total rows: {row_count}")
                logger.info(f"Valid rows: {valid_rows}")
                logger.info(f"Rows with < 500 events: {min_events_violations}")

                if min_events_violations > 0:
                    logger.warning(f"⚠ Found {min_events_violations} rows violating minimum events requirement")

        return validation_results

    except Exception as e:
        logger.error(f"Error validating CSV: {str(e)}")
        return validation_results


def find_csv_file(output_path):
    """Find the actual CSV file in the output directory"""
    path = Path(output_path)

    if path.is_file() and path.suffix == '.csv':
        return str(path)

    if path.is_dir():
        # Look for CSV files in directory
        csv_files = list(path.glob("*.csv"))
        if csv_files:
            return str(csv_files[0])

        # Look for part files (Spark output)
        part_files = list(path.glob("part-*.csv"))
        if part_files:
            return str(part_files[0])

    return None


def main():
    """Main validation function"""
    output_path = os.getenv("OUTPUT_PATH", "/tmp/percentile_results")

    logger.info("=" * 60)
    logger.info("Percentile Query Output Validation")
    logger.info("=" * 60)
    logger.info(f"Output path: {output_path}")
    logger.info("")

    # Find CSV file
    csv_file = find_csv_file(output_path)

    if not csv_file:
        logger.error(f"Could not find CSV file in: {output_path}")
        logger.info("Please check the output path or run the query first")
        sys.exit(1)

    logger.info(f"Found CSV file: {csv_file}")
    logger.info("")

    # Validate
    results = validate_csv_file(csv_file)

    # Print summary
    logger.info("")
    logger.info("=" * 60)
    logger.info("Validation Summary")
    logger.info("=" * 60)

    all_passed = all([
        results.get("file_exists", False),
        results.get("has_header", False),
        results.get("columns_match", False),
        results.get("has_data", False),
        results.get("data_valid", False),
        results.get("min_events_check", False)
    ])

    for check, result in results.items():
        status = "✓ PASS" if result else "✗ FAIL"
        logger.info(f"{status}: {check}")

    logger.info("")

    if all_passed:
        logger.info("✓ All validations passed!")
        sys.exit(0)
    else:
        logger.warning("⚠ Some validations failed. Please review the output above.")
        sys.exit(1)


if __name__ == "__main__":
    main()

