# Makefile pour le projet SGBD

# Variables
JAVAC = javac
JAVA = java
CLASSPATH = .:Code\ Introductif:Table\ Disque\ et\ exemples
MAIN_CLASS = Main

# Fichiers source
SRC_FILES = *.java Code\ Introductif/*.java Table\ Disque\ et\ exemples/*.java

# Target par défaut
all: compile

# Compilation
compile:
	$(JAVAC) -cp "$(CLASSPATH)" -d . $(SRC_FILES)

# Exécution
run: compile
	$(JAVA) -cp "$(CLASSPATH)" $(MAIN_CLASS)

# Nettoyage des fichiers .class
clean:
	rm -f *.class
	rm -f Code\ Introductif/*.class
	rm -f Table\ Disque\ et\ exemples/*.class

# Nettoyage complet et recompilation
rebuild: clean compile

# Affichage de l'aide
help:
	@echo "Targets disponibles :"
	@echo "  all      : Compile le projet (défaut)"
	@echo "  compile  : Compile le projet"
	@echo "  run      : Compile et exécute le programme"
	@echo "  clean    : Supprime les fichiers .class"
	@echo "  rebuild  : Nettoie et recompile"
	@echo "  help     : Affiche cette aide"

.PHONY: all compile run clean rebuild help