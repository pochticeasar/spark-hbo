# Окружение проекта spark-hbo. Подключать в каждом новом терминале:
#   source env.sh
# Java 17 из Homebrew (не трогает системную Java 26)
export JAVA_HOME="$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"
# Python-окружение проекта
[ -d .venv ] && source .venv/bin/activate
echo "java:    $(java -version 2>&1 | head -1)"
echo "python:  $(python --version 2>&1)"
python -c "import pyspark; print('pyspark:', pyspark.__version__)" 2>/dev/null || echo "pyspark: not installed (pip install -r requirements.txt)"
