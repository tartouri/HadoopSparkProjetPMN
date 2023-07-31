
#!/bin/bash

if [ -d "input" ]; then
	if [ -z "$(ls -A input)" ]; then
		echo "depot vide"
	else
		echo "depot non vide"
	fi
fi
