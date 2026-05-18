FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY tetouan_energy_simulator.py .

CMD ["python", "tetouan_energy_simulator.py"]

