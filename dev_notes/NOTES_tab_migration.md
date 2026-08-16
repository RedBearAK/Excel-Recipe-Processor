# NOTES: compact tab migration + sheet_state (2026-08-15)

## The new tab set, in the new ORDER (user-specified)

    VMS            (was VMS_Data)                yellow, unchanged
    Exp_View       (was Export_View)             pale yellow, unchanged
    Exp_Summ       (was Export_Summary)          blues, unchanged
    Exp_Summ_CMA   (was Export_Summary_CMA)      blues, unchanged
    No_Price_Summ  (was No_Price_Product_Summary)
    Cust_Summ      (was Customer_Summary)
    PIDs           (was Product_IDs)             gradient 3D7A3D
    SOs            (was Sales_Orders)            gradient 588E58
    Regions        (was Plant_Origin_to_Regions) gradient 73A173  HIDDEN
    Exp_Dests      (was Export_Destinations)     gradient 8EB58E
    Carriers       (unchanged)                   gradient A9C9A9  HIDDEN
    Can_Sizes      (was CanSize_PackSize)        gradient C4DCC4  HIDDEN
    Cust_List      (was Customer_List, MOVED     gradient DFF0DF  HIDDEN
                    from after Cust_Summ)
    Sources        (was Source_Files)            uncolored        HIDDEN

Lookup-family tab colors are now a 7-step gradient, darkest at PIDs
stepping light rightward, ending at the uncolored Sources - RGB
interpolation (61,122,61)->(223,240,223).

## sheet_state (new format_excel sheet option)

visible / hidden / very_hidden, translated to openpyxl's camelCase
veryHidden. Guided error names the enum and warns that very_hidden
tabs only re-show via VBA or the file format. Documented in the
examples file (new sheet_option_parameter_details section, which also
back-fills zoom_percent - it predated the update-the-docs habit).
Test 7/7 including a save/reload persistence check of both hidden
states. Excel requires >= 1 visible sheet; the active sheet (VMS)
stays visible.

## Migration mechanics (incident-doctrine compliant)

Operations in order: (1) sheets_to_create REORDER, (2) RENAME sweep -
13 names, 78 sites, count-asserted per name, longest-first so
Exp_Summ_CMA could not be bitten by the Exp_Summ replacement,
word-boundary tokens so the CanSize COLUMN vocabulary in the lookup
file was untouched by the Can_Sizes TAB rename, (3) colors + hides.

TWO SHIELDED EXCLUSIONS, restored verbatim after the sweep:
- The source-workbook history comment (was "Sales_Orders" names THEIR
  old tab, not ours).
- THE CROSS-RECIPE INTERFACE: the standalone CMA lookup file's sheet
  stays "Export_Summary_CMA" - the CMA invoice recipe imports it BY
  THAT NAME. The workbook display tab renamed; the interface did not.
  A comment now marks the site so no future sweep renames it blindly.

Two mid-migration defects caught by verification, both fixed: the
first color/hide pass matched the sheets_to_create entries instead of
the formatting entries (stripped, redone against the formatting
block); and the DEMO's import had to shield its source-file tab name
(the source workbook still says VMS_Data - only OUR output tab is
VMS).

Integration-verified on the regenerated demo: tabs renamed and
ordered, Cust_List stored hidden, the DV spill source followed the
rename (_xlfn.ANCHORARRAY(Cust_List!$A$2)), named ranges retargeted
VMS!, GROUPBY intact, whole-file grammar audit CLEAN.

## Queued (user note during this migration)

Evaluate making Exp_Summ and Exp_Summ_CMA spilled-function tabs (the
Exp_View pattern applied to the summaries) instead of exported static
frames.
