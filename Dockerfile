FROM python:3.11-slim

WORKDIR /app

COPY requirements_master.txt .
RUN pip install --no-cache-dir -r requirements_master.txt

# Instalar Chromium + todas sus dependencias de sistema
RUN playwright install chromium --with-deps

EXPOSE 5005

CMD ["python", "trends/app.py"]
