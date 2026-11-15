"""Presentation only: plot complete SQL-returned synthetic FHIR populations."""

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.colors import ListedColormap
from matplotlib.patches import Rectangle
from vgi_notebooks import finish

SOURCE = "Authored synthetic FHIR R4 resources · actual VGI FHIR v0.4.0 native release · loopback only\nNo patient records; technical data-shape checks do not establish clinical validity."


def reasons(frame):
    fig, ax = plt.subplots(figsize=(11, 6.5))
    labels = [
        str(x)
        .replace("Outside final-status scope", "Outside final-\nstatus scope")
        .replace("Unsupported unit/system", "Unsupported\nunit/system")
        for x in frame.inclusion_reason
    ]
    bars = ax.barh(
        range(len(frame)),
        frame.observations,
        color=[
            "#327659" if x == "Included exact quantity" else "#A4B3AC"
            for x in frame.inclusion_reason
        ],
    )
    ax.bar_label(bars, padding=5)
    ax.set_yticks(range(len(frame)), labels, fontsize=9)
    ax.set(
        xlabel="Synthetic observations (count)", xlim=(0, frame.observations.max() + 1)
    )
    ax.set_xticks(range(int(frame.observations.max()) + 2))
    ax.invert_yaxis()
    finish(
        fig,
        "Complete retrieval still needs a comparison policy",
        "All 12 observations retained · first applicable reason in the visible SQL CASE",
        SOURCE,
        name="01-inclusion-reasons",
    )


def normalized(frame):
    fig, ax = plt.subplots(figsize=(11, 5.3))
    ax.scatter(frame.normalized_kg, range(len(frame)), s=85, color="#327659")
    ax.set_yticks(
        range(len(frame)), [f"{r.id} · {r.patient_id}" for r in frame.itertuples()]
    )
    for i, r in enumerate(frame.itertuples()):
        ax.annotate(
            f"{r.original_value:g} {r.original_unit} → {r.normalized_kg:g} kg",
            (r.normalized_kg, i),
            xytext=(10, 0),
            textcoords="offset points",
            va="center",
            fontsize=10,
        )
    maximum = max(1, float(frame.normalized_kg.max()))
    ax.set(
        xlabel="Normalized quantity (kg) · deliberately synthetic",
        xlim=(-0.05 * maximum, maximum * 1.65),
    )
    ax.margins(y=0.4)
    ax.invert_yaxis()
    finish(
        fig,
        "Preserve zero while converting supported units",
        "Three eligible quantities · one explicit zero · no absent quantity is imputed",
        SOURCE,
        name="02-normalized-quantities",
    )


def shapes(frame, selected):
    columns = [
        "has_numeric_quantity",
        "has_text_value",
        "has_comparator",
        "has_ucum_system",
        "relative_patient_reference",
    ]
    assert frame[columns].notna().all().all()
    matrix = frame[columns].astype(int).to_numpy()
    fig, ax = plt.subplots(figsize=(11, 8.5))
    ax.imshow(
        matrix,
        cmap=ListedColormap(["#EDF1EF", "#327659"]),
        vmin=0,
        vmax=1,
        aspect="auto",
    )
    ax.set_xticks(
        range(len(columns)),
        [
            "Numeric\nquantity",
            "Text\nvalue",
            "Quantity\ncomparator",
            "UCUM\nsystem",
            "Relative\nPatient ref.",
        ],
        fontsize=10,
    )
    ax.set_yticks(
        range(len(frame)),
        [f"{r.id} · {r.numeric_state}" for r in frame.itertuples()],
        fontsize=9,
    )
    for i in range(len(frame)):
        for j in range(len(columns)):
            ax.text(
                j,
                i,
                "yes" if matrix[i, j] else "no",
                ha="center",
                va="center",
                fontsize=9,
                color="white" if matrix[i, j] else "#627468",
            )
    ax.set_xticks(np.arange(-0.5, len(columns), 1), minor=True)
    ax.set_yticks(np.arange(-0.5, len(frame), 1), minor=True)
    ax.grid(which="minor", color="white", linewidth=2)
    ax.tick_params(which="minor", bottom=False, left=False)
    i = frame.id.tolist().index(selected)
    ax.add_patch(
        Rectangle(
            (-0.48, i - 0.48),
            len(columns) - 0.04,
            0.96,
            fill=False,
            edgecolor="#B16D44",
            linewidth=2.5,
        )
    )
    ax.set_xlabel(
        "Returned JSON fields / predicate results · yes/no, not a validity score"
    )
    finish(
        fig,
        "A missing numeric quantity can retain another value",
        f"All 12 resources · outlined row {selected} was independently read by ID",
        SOURCE
        + "\nRelative-reference presence does not establish that its target exists.",
        name="03-resource-shapes",
    )
