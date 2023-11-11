# 5verst-telegram-bot

# Start

To have references: https://mastergroosha.github.io/aiogram-3-guide/quickstart/

To create `env` directory:

```
python -m venv env
```

To start virtual environment (Windows PowerShell):

```
.\env\Scripts\Activate.ps1
```

To deactivate simply execute:

```
deactivate
```

Tosave deps:

```
pip freeze > requirements.txt
```

To reinstall deps:

```
pip install -r requirements.txt
```

Before run:

1. Create `.env` file
2. Put `API_TOKEN` variable in file
3. Apply `.env` file after activation of virtual environment

To run:

```
python ./main.py
```

To test:
Open telegram app and send messages to "Хранитель 5 вёрст"