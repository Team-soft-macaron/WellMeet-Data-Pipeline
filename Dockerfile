FROM public.ecr.aws/lambda/python:3.12
RUN dnf update -y && dnf install -y \
    wget \
    nss \
    nspr \
    atk \
    at-spi2-atk \
    cups-libs \
    libxkbcommon \
    at-spi2-core \
    libXcomposite \
    libXdamage \
    libXfixes \
    libXrandr \
    mesa-libgbm \
    pango \
    cairo \
    alsa-lib \
    && dnf clean all
COPY requirements.txt .
RUN pip install -r requirements.txt && playwright install chromium

COPY . .

CMD ["lambda_function.handler"]