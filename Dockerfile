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
ENV PLAYWRIGHT_BROWSERS_PATH=/opt/playwright-browsers
RUN pip install -r requirements.txt && playwright install

COPY . .

CMD ["lambda_function.handler"]