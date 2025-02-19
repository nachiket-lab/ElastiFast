# Stage 1: Build dependencies using a lightweight base
FROM python:3.11-alpine AS builder

# Install build dependencies
RUN apk add --no-cache \
    gcc \
    musl-dev \
    libffi-dev \
    libpq \
    libpq-dev \
    curl \
    make

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    APP_HOME=/app

WORKDIR $APP_HOME

# Install Poetry
RUN curl -sSL https://install.python-poetry.org | python3 - \
    && export PATH="/root/.local/bin:$PATH"

# Copy dependency files
COPY pyproject.toml poetry.lock ./

# Build wheels for dependencies
RUN export PATH="/root/.local/bin:$PATH" \
    && pip install wheel \
    && poetry install && \
    && poetry export --without-hashes -f requirements.txt -o requirements.txt \
    && pip wheel --no-cache-dir --wheel-dir=/wheels -r requirements.txt

# Stage 2: Final runtime image
FROM python:3.11-alpine AS production

# Install runtime dependencies
RUN apk add --no-cache \
    libstdc++
#    libpq \

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    APP_HOME=/app

WORKDIR $APP_HOME

# Copy wheels from builder stage
COPY --from=builder /wheels /wheels

# Install Python dependencies from wheels
RUN pip install --no-cache-dir --no-index --find-links=/wheels /wheels/*

# Copy application code
COPY . .

# Expose the FastAPI port
EXPOSE 8000

# Default command to run the application
CMD ["uvicorn", "elastifast.main:app", "--host", "0.0.0.0", "--port", "8000"]