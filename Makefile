.PHONY: init clean

PYTHON=/usr/bin/python3.8

init:
	${PYTHON} -m pip install -r requirements.txt
	${PYTHON} -m pip install -r scripts/requirements.txt

clean:
	rm -r .cache/
	rm -r .logs/
	rm -r .report/