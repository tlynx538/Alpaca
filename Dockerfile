FROM alpine:latest 
WORKDIR /home/Alpaca 

# Copy Files 
COPY api/ .

# Install Dependencies
RUN apk add --no-cache python3 python3-dev py3-pip build-base linux-headers
RUN python3 -m venv /venv
ENV PATH="/venv/bin:$PATH"
RUN pip install --no-cache-dir "fastapi[standard]" jupyter_client ipykernel


