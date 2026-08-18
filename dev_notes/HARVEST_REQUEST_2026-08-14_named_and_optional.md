# HARVEST REQUEST: named constructs, optional params, lambda helpers
# (2026-08-14)

Build in Excel 365 (current channel), confirm every formula CALCULATES
before saving (a formula Excel refuses to even enter is itself
information - note it and skip), save as .xlsx, upload. Each item pins
storage bytes we currently generate on inference or refuse pending
exactly this file.

## Data setup (Sheet1)

    A1:A6  Alpha, Beta, Alpha, Gamma, Beta, Alpha
    B1:B6  10, 20, 30, 40, 50, 60

## Name Manager entries (Formulas -> Name Manager -> New)

 1. HarvestLam     =LAMBDA(x,y,x*y)
      Named multi-param lambda from the true oracle: pins the
      '='-less definedName content and multi-parameter declaration.
 2. HarvestLet     =LET(a,2,b,3,a*b)
      A named formula that IS a LET - we now generate these with zero
      real-Excel bytes behind them.
 3. HarvestLamLet  =LAMBDA(v,LET(t,v*2,t+v))
      LET nested inside a named lambda body.

## Cell formulas

 4. D1  =HarvestLam(3,4)         (calling cell for a named lambda)
 5. D2  =HarvestLet              (calling cell for a named LET)
 6. E1  =LAMBDA(x,[y],IF(ISOMITTED(y),x,x+y))(5)
      THE optional-parameter harvest - the transformer refuses [y]
      pending these bytes; also pins ISOMITTED storage.
 7. E2  =LAMBDA(x,[y],IF(ISOMITTED(y),x,x+y))(5,7)
      Same lambda with the optional supplied.
 8. F1  =PIVOTBY(A1:A6,A1:A6,B1:B6,SUM)
      xlsxwriter's table leaves PIVOTBY bare (lags 2024 functions);
      ours says _xlfn. on family inference - settle it.
 9. F6  =PERCENTOF(B1:B2,B1:B6)
      Same situation as PIVOTBY.
10. G1  =MAP(A1:A3,LAMBDA(txt,LEN(txt)))
      Lambda as an argument to a helper function; MAP is not in our
      prefix map yet - this adds it with real bytes.
11. G6  =REDUCE(0,B1:B3,LAMBDA(acc,val,acc+val))
      Two-parameter lambda inside REDUCE (also unmapped).
12. H1  =LET(seq,SEQUENCE(3),SUM(seq))
      Cell-level LET wrapping a future function, from Excel itself.
13. H6  =LAMBDA(my_val,my_val*2)(10)
      Underscore parameter-name bytes.

On arrival: the file joins tests/fixtures/, the grammar-corpus test
grows one case per item, MAP/REDUCE (and PIVOTBY/PERCENTOF if
confirmed) get pinned map entries, and optional [param] support gets
implemented against items 6-7 instead of inference.
