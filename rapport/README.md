# Rapport LaTeX — Sections 15, 17 et 18

Ce dossier contient le code LaTeX des sections **Éthique & Conformité**, **Perspectives d'amélioration** et **Conclusion** du rapport final du projet *Fondamentaux de Big Data* (M126).

## Compilation

### Option 1 — Overleaf (le plus simple, sans installation)
1. Allez sur [overleaf.com](https://www.overleaf.com), créez un compte gratuit.
2. **New Project → Upload Project**, et déposez le fichier `main.tex`.
3. Cliquez sur **Recompile**. Le PDF est généré en quelques secondes.

### Option 2 — En local (Linux/Mac/Windows)
Prérequis : une distribution LaTeX (TeX Live, MiKTeX ou MacTeX).

```bash
cd rapport/
pdflatex main.tex
pdflatex main.tex   # 2e passe pour mettre à jour la table des matières
```

Le fichier généré est `main.pdf`.

### Option 3 — Avec latexmk (recommandé)
```bash
latexmk -pdf main.tex
latexmk -c          # nettoyage des fichiers auxiliaires
```

## Intégration dans un rapport plus large

Si vous voulez intégrer ces sections dans un rapport complet existant :

1. Copiez **uniquement le contenu entre `\begin{document}` et `\end{document}`**, en excluant la page de titre et la table des matières.
2. Vérifiez que votre préambule contient les packages utilisés (voir la liste ci-dessous).
3. Adaptez les numéros de section si nécessaire (`\setcounter{section}{14}` avant la première `\section`).

### Packages requis
- `inputenc`, `fontenc`, `lmodern`, `babel` (français), `csquotes`
- `geometry`, `setspace`, `parskip`
- `xcolor`, `titlesec`
- `booktabs`, `tabularx`, `longtable`, `array`, `multirow`, `colortbl`
- `enumitem`, `pifont`
- `hyperref`, `fancyhdr`
- `tcolorbox` (avec l'option `most`)

## Structure du document

| Section | Titre | Pages estimées |
|---|---|---|
| 15 | Éthique & Conformité | ~4 pages |
| 17 | Perspectives d'amélioration | ~4 pages |
| 18 | Conclusion | ~3 pages |
| **Total** | | **~11 pages** |

## Personnalisation rapide

- **Changer les couleurs** : modifiez les définitions `\definecolor{...}` au début du fichier.
- **Changer la police** : remplacez `\usepackage{lmodern}` par `\usepackage{kpfonts}` ou `\usepackage{libertine}`.
- **Ajouter un logo ENSA** : insérez `\includegraphics[width=4cm]{logo-ensa.png}` dans la page de titre.
- **Imprimer en recto-verso** : ajoutez l'option `twoside` à `\documentclass`.
