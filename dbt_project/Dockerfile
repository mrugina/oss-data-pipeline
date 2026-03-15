FROM python:3.9-slim

# Set the working directory
WORKDIR /usr/app

# Install dbt and the PostgreSQL adapter
RUN pip install dbt-postgres

# Copy the dbt project to the container
COPY . .

ENTRYPOINT ["dbt"]
CMD ["run", "--profiles-dir", "."]

