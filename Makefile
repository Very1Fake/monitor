.PHONY: init clean

PYTHON=/usr/bin/python3.8

init:
	sudo ${PYTHON} -m pip install -r requirements.txt

clean:
	rm -r cache/
	rm -r reports/
	rm -r logs/