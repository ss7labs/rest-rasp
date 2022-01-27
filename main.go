package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"regexp"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/mux"
	_ "github.com/mailru/go-clickhouse"
	_ "github.com/shopspring/decimal"
)

type Kv struct {
	Calls    []Call     `json:"calls"`
	Iskv     bool       `json:"iskv"`
	Locals   LocalTotal `json:"locals"`
	Notfound bool       `json:"notfound"`
	Info     AccInfo    `json:"info"`
	ExtTotal float64    `json:"exttotal"`
	Total    float64    `json:"total"`
}

type Call struct {
	Time      string  `json:"time"`
	Numa      string  `json:"numa"`
	Numb      string  `json:"numb"`
	Direction string  `json:"direction"`
	Duration  int64   `json:"duration" gorm:"type:numeric"`
	Minutes   int64   `json:"minutes" gorm:"type:numeric"`
	Cost      float32 `json:"cost" gorm:"type:numeric"`
	Iskz      bool    `json:"iskz"`
}
type LocalTotal struct {
	Duration int64   `json:"duration" gorm:"type:numeric"`
	Total    float64 `json:"total" gorm:"type:numeric"`
}

type AccInfo struct {
	Name     string `json:"name"`
	Street   string `json:"street"`
	Settl    string `json:"settl"`
	Building string `json:"building"`
	Flat     string `json:"flat"`
}

var calls []Call
var chdb *sql.DB
var lbdb *sql.DB
var asudb *sql.DB
var err error
var orderedPrefix []string

func main() {

	lbdb, err = sql.Open("mysql", "update:some_pass890@tcp(10.19.80.34:33062)/lb")
	if err != nil {
		log.Fatal(err)
	}
	defer lbdb.Close()

	asudb, err = sql.Open("mysql", "asu:qwerty@tcp(localhost:3308)/agts_asu")
	if err != nil {
		log.Fatal(err)
	}
	defer asudb.Close()

	chdb, err = sql.Open("clickhouse", "http://default:te410pte412p@10.19.64.124:8123/default?debug=true")
	if err != nil {
		log.Fatal(err)
	}
	defer chdb.Close()

	loadPrefixes()

	//calls = append(calls, Call{Numa: "429541", Numb: "329542", Duration: "12", Cost: "1.6"})
	router := mux.NewRouter()
	router.HandleFunc("/api-kv/{numa}/{from}/{to}", getKv).Methods("GET")
	http.ListenAndServe(":4242", router)
}

func getKv(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	params := mux.Vars(r)
	var kv Kv
	query := "select IF(COUNT(*),'false','true') from phones WHERE phone='" + params["numa"] + "'"
	kv.Notfound = isPhoneAvail(query)

	if kv.Notfound {
		json.NewEncoder(w).Encode(kv)
		return
	}

	query = "select org_id from phones WHERE phone='" + params["numa"] + "'"
	kv.Iskv = isKv(query)

	if !kv.Iskv {
		json.NewEncoder(w).Encode(kv)
		return
	}
	//fill info
	query = "SELECT name, IFNULL(street,''), IFNULL(settl,''), building, IFNULL(flat,'') FROM 09_address WHERE number='" + params["numa"] + "'"
	kv.Info = getInfo(query)
	//fill Locals
	query = "select sum(exc_dur),sum(rate_man) from asubill.rated where numa='" + params["numa"] + "' AND toYYYYMMDD(event_date) BETWEEN " + params["from"] + " AND " + params["to"] + " AND numb NOT LIKE '8%' GROUP BY numa"
	kv.Locals = makeLocalTotal(query)
	//fill Calls
	query = "select event_time,numa,numb,duration,rate_man,kz,minutes from asubill.rated where numa='" + params["numa"] + "' AND toYYYYMMDD(event_date) BETWEEN " + params["from"] + " AND " + params["to"] + " AND numb LIKE '8%' ORDER BY event_date"
	kv.Calls = retriveExtCalls(query)
	//fill Total
	query = "select sum(rate_man) from asubill.rated where numa='" + params["numa"] + "' AND toYYYYMMDD(event_date) BETWEEN " + params["from"] + " AND " + params["to"] + " GROUP BY numa"
	kv.Total = retriveTotal(query)

	json.NewEncoder(w).Encode(kv)
}

func retriveTotal(query string) float64 {
	var total float64
	err := chdb.QueryRow(query).Scan(&total)
	if err != nil {
		if err == sql.ErrNoRows {
			total = 0
		} else {
			panic(err.Error())
		}
	}
	total = math.Round(total*1000) / 1000
	fmt.Println(total)
	return total
}

func isPhoneAvail(query string) bool {
	var notFound bool
	err := asudb.QueryRow(query).Scan(&notFound)
	if err != nil {
		panic(err.Error())
	}
	return notFound
}

func isKv(query string) bool {
	isKv := false
	org_id := 0
	err := asudb.QueryRow(query).Scan(&org_id)
	if err != nil {
		if org_id == 0 && err != sql.ErrNoRows {
			isKv = true
		}
	}
	return isKv
}
func retriveExtCalls(query string) []Call {
	var calls []Call
	rows, err := chdb.Query(query)
	if err != nil {
		panic(err.Error())
	}
	defer rows.Close()

	for rows.Next() {
		var call Call
		if err := rows.Scan(&call.Time, &call.Numa, &call.Numb, &call.Duration, &call.Cost, &call.Iskz, &call.Minutes); err != nil {
			log.Fatal(err)
		}

		/*
		   rem := call.Duration % 60
		   if rem < 7 {
		     call.Duration = call.Duration - rem
		   }
		*/
		if call.Duration%60 == 0 {
			call.Duration = call.Duration / 60
		} else {
			min := float64((call.Duration / 60 * 100) / 100)
			call.Duration = int64(math.Ceil(min) + 1)
		}

		call.Direction = getDirection(call.Numb)
		calls = append(calls, call)
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
	return calls
}

func getDirection(number string) string {
	direction := ""

	for _, value := range orderedPrefix {
		regex := "^" + value + "[0-9]+"
		matched, _ := regexp.MatchString(regex, number)
		if matched {
			//fmt.Println(value, s[1])
			direction = getNameOfPrefix(value)
			break
		}
	}

	return direction
}
func getNameOfPrefix(prefix string) string {
	output := ""
	query := "SELECT name FROM route_prices WHERE prefix='" + prefix + "'"
	err := asudb.QueryRow(query).Scan(&output)
	if err != nil {
		if err == sql.ErrNoRows {
			output = ""
		} else {
			panic(err.Error())
		}
	}
	return output
}

func makeLocalTotal(query string) LocalTotal {
	var min int64
	var total float64
	var locals LocalTotal
	var (
		exc_dur  int64
		rate_man float64
	)
	err := chdb.QueryRow(query).Scan(&exc_dur, &rate_man)
	if err != nil {
		if err == sql.ErrNoRows {
			total = 0
		} else {
			panic(err.Error())
		}
	}
	min = exc_dur / 60
	total = math.Round(rate_man*1000) / 1000
	if total < 0.01 {
		locals.Total = 0
		locals.Duration = 0
	} else {
		locals.Total = total
		locals.Duration = min
	}
	//fmt.Println("Local",exc_dur,rate_man)
	return locals
}

func makeTotal(w http.ResponseWriter, query string) {
	var min int64
	var total float64
	var locals LocalTotal
	var (
		exc_dur  int64
		rate_man float64
	)
	err := chdb.QueryRow(query).Scan(&exc_dur, &rate_man)
	if err != nil {
		if err == sql.ErrNoRows {
			total = 0
		} else {
			panic(err.Error())
		}
	}
	min = exc_dur / 60
	total = math.Round(rate_man*1000) / 1000
	if total < 0.01 {
		locals.Total = 0
		locals.Duration = 0
	} else {
		locals.Total = total
		locals.Duration = min
	}
	//fmt.Println("Local",exc_dur,rate_man)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(locals)
}
func getInfo(query string) AccInfo {
	var info AccInfo
	err := lbdb.QueryRow(query).Scan(&info.Name, &info.Street, &info.Settl, &info.Building, &info.Flat)
	if err != nil {
		if err == sql.ErrNoRows {
			info.Name = ""
			info.Street = ""
			info.Settl = ""
			info.Building = ""
			info.Flat = ""
		} else {
			panic(err.Error())
		}
	}
	return info
}

func loadPrefixes() {
	query := "SELECT prefix FROM route_prices ORDER BY CHAR_LENGTH(prefix) DESC"

	rows, err := asudb.Query(query)
	if err != nil {
		panic(err.Error())
	}
	defer rows.Close()

	for rows.Next() {
		var prefix string
		if err := rows.Scan(&prefix); err != nil {
			panic(err.Error())
		}
		orderedPrefix = append(orderedPrefix, prefix)
	}
}
