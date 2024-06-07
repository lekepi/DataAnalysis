from models import Product, session, engine
import pandas as pd


if __name__ == '__main__':
    exclude_list = []
    ticker_string = "db1, ibkr, ice, lseg, spgi, v, ma, adyen, pypl, bro, byd, csu, toi, lmn, instal, lifcob, rop, kws, brk/b, adbe, adsk, amzn, anss, cert, crm, wday, epam, estc, goog, lvgo, meta, msft, ui, chwy, w, cpr, dge, rco, ri, fevr, nke, pum, ads, onon, barn, bn, lisp, nesn, ad, ca, dg, wmt, tsco, dpz, mcd, mty, 7974, atvi, ntdoy, ttwo, mc, rms, bc, sfer, ker, cfr, chr, crda, dsfir, givn, iff, kyga, nsisb, rbt, sy1, de, scha, auto, g24, rmv, spr, bakka, mowi, aht, assab, doka, ferg, gebn, knebv, ksp, lr, rxl, sgo, sika, air, mtx, saf, epia, metso, ror, sand, weir, cni, cp, csx, nsc, af, lha, jet2, rya, tui, wizz, bdev, bkg, len, nvr, psn, rmv, xhb, bvi, itrk, sgsn, dhr, dim, srt3, mrk, spx, tmo, wat, a, lonn, afx, alc, bim, demant, el, soon, stmn, erf, grf, grf/p, alc, coo, ew, isrg, masi, syk"
    ticker_list = ticker_string.split(", ")

    product_db = session.query(Product).all()

    for ticker in ticker_list:
        ticker_temp = ticker.upper() + " "
        # start with ticker
        prod_matches = [prod for prod in product_db if prod.ticker.startswith(ticker_temp) if prod.prod_type == 'Cash']
        if prod_matches:
            bbg_ticker = prod_matches[0].ticker
            exclude_list.append(bbg_ticker)
        else:
            print(f"Ticker {ticker_temp} not found in database")

    my_sql = "SELECT distinct(T2.ticker) as ticker FROM position T1 JOIN product T2 on T1.product_id=T2.id WHERE prod_type='Cash' and entry_date>='2019-04-01' and parent_fund_id=1 order by entry_date;"
    df = pd.read_sql(my_sql, con=engine)

    # get the list
    db_ticker_list = df['ticker'].tolist()
    # remove result_list from db_ticker_list
    result_list = list(set(db_ticker_list) - set(exclude_list))

    # string with all tickers with + between them
    result_string = "+".join(result_list)

    print(result_string)
    # put string in text file
    with open(r"H:\Python Output\TEST\Ticker List.txt", "w") as file:
        file.write(result_string)


