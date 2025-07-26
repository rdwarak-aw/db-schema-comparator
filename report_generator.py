from jinja2 import Environment, FileSystemLoader
from weasyprint import HTML
import os
import json
from datetime import datetime

def generate_html_report(diff_report, output_path, logger, src_info=None, dst_info=None):
    try:
        #print(json.dumps(diff_report, indent=2))
        template_dir = os.path.join(os.path.dirname(__file__), "templates")
        env = Environment(loader=FileSystemLoader(template_dir))
        template = env.get_template("report_html_template.html")

        context = {
            "diff": diff_report,
            "source": src_info,
            "destination": dst_info,
            "timestamp": datetime.now().strftime("%d %b %Y %H:%M:%S")
        }

        html_content = template.render(**context)

        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html_content)

        logger.info(f"HTML report generated at: {output_path}")
    except Exception as e:
        logger.exception(f"Failed to generate HTML report: {str(e)}")

def generate_pdf_report(diff_report, output_path, logger, src_info=None, dst_info=None):
    try:
        env = Environment(loader=FileSystemLoader("templates"))
        template = env.get_template("report_pdf_template.html")

        context = template.render(
            diff=diff_report,
            source=src_info,
            destination=dst_info,
            timestamp=datetime.now().strftime("%d %b %Y %H:%M:%S")
        )

        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        HTML(string=context).write_pdf(output_path)
        logger.info(f"PDF report generated at: {output_path}")
    except Exception as e:
        logger.exception(f"Failed to generate PDF report: {str(e)}")