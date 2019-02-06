#
# doi-automation
#
FROM python:3.6

#  Set the working directory
WORKDIR /app
ENV PYTHONPATH /app/src

# Copy the current directory contents into the container at /app
COPY . /app

#  Update python3-pip
RUN python -m pip install pip --upgrade

#  Install python packages specified in requirements.txt
RUN pip install --trusted-host pypi.python.org -r requirements.txt

# Run app.py when the container launches
CMD ["python", "./update.py"]