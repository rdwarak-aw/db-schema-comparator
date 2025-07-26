from config_loader import load_config
from logger import setup_logger
from db_factory import get_db_adapter
from comparator import compare_metadata
from report_generator import generate_html_report, generate_pdf_report
import os

def main():
    logger = setup_logger()
    config = load_config()
    logger.info("Starting DB schema comparison...")

    try:
        active_db = config.get("active_db")
        db_config = config.get(active_db)
        if not db_config:
            raise ValueError(f"Missing configuration for active_db: {active_db}")
        
        logger.info(f"Using database type: {active_db}")

        src_conn_cfg = db_config["source"]
        dst_conn_cfg = db_config["destination"]
        schemas = src_conn_cfg.get("schemas", [])

        src_info = {
            "server": src_conn_cfg["server"],
            "database": src_conn_cfg["database"]
        }
        dst_info = {
            "server": dst_conn_cfg["server"],
            "database": dst_conn_cfg["database"]
        }

        # Inject schema list into each adapter's config for metadata extraction
        src_adapter = get_db_adapter(active_db, {**config, "schemas_to_compare": schemas}, logger)
        dst_adapter = get_db_adapter(active_db, {**config, "schemas_to_compare": schemas}, logger)

        src_adapter.connect(src_conn_cfg)
        dst_adapter.connect(dst_conn_cfg)

        src_meta = src_adapter.extract_metadata()
        dst_meta = dst_adapter.extract_metadata()

        diff_report = compare_metadata(src_meta, dst_meta, config, logger)

        report_formats = config.get("output", {}).get("formats", ["html"])

        if "html" in report_formats:
            report_path = config["output"]["html_report"]
            os.makedirs(os.path.dirname(report_path), exist_ok=True)
            generate_html_report(diff_report, report_path, logger, src_info, dst_info)

        if "pdf" in report_formats:
            report_path = config["output"]["pdf_report"]
            os.makedirs(os.path.dirname(report_path), exist_ok=True)
            generate_pdf_report(diff_report, report_path, logger, src_info, dst_info)

        logger.info(f"Schema diff completed. Report saved to: {report_formats}")
    except Exception as e:
        logger.exception(f"Unhandled error during execution: {str(e)}")
    finally:
        if 'src_adapter' in locals(): src_adapter.close()
        if 'dst_adapter' in locals(): dst_adapter.close()

if __name__ == "__main__":
    main()
