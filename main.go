package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type IHPRegistrations struct {
	IncidentType                  string
	DeclarationDate               time.Time
	DisasterNumber                string `gorm:"index"`
	County                        string `gorm:"index"`
	DamagedStateAbbreviation      string `gorm:"index"`
	DamagedCity                   string
	DamagedZipCode                string `gorm:"index"`
	ApplicantAge                  string
	householdComposition          string
	OccupantsUnderTwo             string
	Occupants2to5                 string
	Occupants6to18                string
	Occupants19to64               string
	Occupants65andOver            string
	GrossIncome                   string
	OwnRent                       string
	PrimaryResidence              bool
	ResidenceType                 string
	HomeOwnersInsurance           bool
	FloodInsurance                bool
	RegistrationMethod            string
	IhpReferral                   bool
	IhpEligible                   bool
	IhpAmount                     float64
	FipAmount                     float64
	HaReferral                    bool
	HaEligible                    bool
	HaAmount                      float64
	HaStatus                      string
	OnaReferral                   bool
	OnaEligible                   bool
	OnaAmount                     float64
	UtilitiesOut                  bool
	HomeDamage                    bool
	AutoDamage                    bool
	EmergencyNeeds                bool
	FoodNeed                      bool
	ShelterNeed                   bool
	AccessFunctionalNeeds         bool
	SbaEligible                   bool
	SbaApproved                   bool
	InspnIssued                   bool
	InspnReturned                 bool
	HabitabilityRepairsRequired   bool
	Rpfvl                         float64
	Ppfvl                         float64
	RenterDamageLevel             string
	Destroyed                     bool
	WaterLevel                    float64
	HighWaterLocation             string
	FloodDamage                   bool
	FloodDamageAmount             float64
	FoundationDamage              bool
	FoundationDamageAmount        float64
	RoofDamage                    bool
	RoofDamageAmount              float64
	TsaEligible                   bool
	TsaCheckedIn                  bool
	RentalAssistanceEligible      bool
	RentalAssistanceAmount        float64
	RepairAssistanceEligible      bool
	RepairAmount                  float64
	ReplacementAssistanceEligible bool
	ReplacementAmount             float64
	PersonalPropertyEligible      bool
	PersonalPropertyAmount        float64
	IhpMax                        bool
	HaMax                         bool
	OnaMax                        bool
	LastRefresh                   time.Time
	ID                            string `gorm:"primaryKey"`
}

func main() {

	// Open Database
	log.Println("Opening Database")
	db, err := gorm.Open(sqlite.Open("ihp.db"), &gorm.Config{})
	if err != nil {
		panic(err)
	}

	// Create Table
	err = db.AutoMigrate(&IHPRegistrations{})
	if err != nil {
		panic(err)
	}

	// open file
	log.Println("Opening CSV file")
	f, err := os.Open("IHP.csv")
	if err != nil {
		log.Fatal(err)
	}

	// remember to close the file at the end of the program
	defer f.Close()

	// read csv values using csv.Reader
	csvReader := csv.NewReader(f)
	// Read the first line, it's just headers.
	_, err = csvReader.Read()
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Skimming CSV file to find length")
	totalToProcess, err := lineCounter(f)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Total lines to process:", totalToProcess)

	f.Seek(0, 0)
	// read csv values using csv.Reader
	csvReader = csv.NewReader(f)
	// Read the first line, it's just headers.
	_, err = csvReader.Read()
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Parsing CSV file")
	rowCount := 0
	for {
		line, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		row := IHPRegistrations{
			IncidentType:             line[0],
			DeclarationDate:          strToDate(line[1]),
			DisasterNumber:           line[2],
			County:                   line[3],
			DamagedStateAbbreviation: line[4],
			DamagedCity:              line[5],
			DamagedZipCode:           line[6],
			ApplicantAge:             line[7],
			householdComposition:     line[8],
			OccupantsUnderTwo:        line[9],
			Occupants2to5:            line[10],
			Occupants6to18:           line[11],
			Occupants19to64:          line[12],
			Occupants65andOver:       line[13],
			GrossIncome:              line[14],
			OwnRent:                  line[15],
			ResidenceType:            line[17],
			RegistrationMethod:       line[20],
			IhpAmount:                strToFloat(line[23]),
			FipAmount:                strToFloat(line[24]),
			HaAmount:                 strToFloat(line[27]),
			HaStatus:                 line[28],
			OnaAmount:                strToFloat(line[31]),
			Rpfvl:                    strToFloat(line[44]),
			Ppfvl:                    strToFloat(line[45]),
			RenterDamageLevel:        line[46],
			WaterLevel:               strToFloat(line[48]),
			HighWaterLocation:        line[49],
			FloodDamageAmount:        strToFloat(line[51]),
			FoundationDamageAmount:   strToFloat(line[53]),
			RoofDamageAmount:         strToFloat(line[55]),
			RentalAssistanceAmount:   strToFloat(line[59]),
			RepairAmount:             strToFloat(line[61]),
			ReplacementAmount:        strToFloat(line[63]),
			PersonalPropertyAmount:   strToFloat(line[65]),
			LastRefresh:              strToDate(line[69]),
			ID:                       line[70],
		}

		primaryResidence, err := strToBool(line[16])
		if err == nil {
			row.PrimaryResidence = primaryResidence
		}
		homeOwnersInsurance, err := strToBool(line[18])
		if err == nil {
			row.HomeOwnersInsurance = homeOwnersInsurance
		}
		floodInsurance, err := strToBool(line[19])
		if err == nil {
			row.FloodInsurance = floodInsurance
		}
		ihpReferral, err := strToBool(line[21])
		if err == nil {
			row.IhpReferral = ihpReferral
		}
		ihpEligible, err := strToBool(line[22])
		if err == nil {
			row.IhpEligible = ihpEligible
		}
		haReferral, err := strToBool(line[25])
		if err == nil {
			row.HaReferral = haReferral
		}
		haEligible, err := strToBool(line[26])
		if err == nil {
			row.HaEligible = haEligible
		}
		onaReferral, err := strToBool(line[29])
		if err == nil {
			row.OnaReferral = onaReferral
		}
		onaEligible, err := strToBool(line[30])
		if err == nil {
			row.OnaEligible = onaEligible
		}
		utilitiesOut, err := strToBool(line[64])
		if err == nil {
			row.UtilitiesOut = utilitiesOut
		}
		homeDamage, err := strToBool(line[33])
		if err == nil {
			row.HomeDamage = homeDamage
		}
		autoDamage, err := strToBool(line[34])
		if err == nil {
			row.AutoDamage = autoDamage
		}
		emergencyNeeds, err := strToBool(line[35])
		if err == nil {
			row.EmergencyNeeds = emergencyNeeds
		}
		foodNeed, err := strToBool(line[36])
		if err == nil {
			row.FoodNeed = foodNeed
		}
		shelterNeed, err := strToBool(line[37])
		if err == nil {
			row.ShelterNeed = shelterNeed
		}
		accessFunctionalNeeds, err := strToBool(line[38])
		if err == nil {
			row.AccessFunctionalNeeds = accessFunctionalNeeds
		}
		sbaEligible, err := strToBool(line[39])
		if err == nil {
			row.SbaEligible = sbaEligible
		}
		sbaApproved, err := strToBool(line[40])
		if err == nil {
			row.SbaApproved = sbaApproved
		}
		inspnIssued, err := strToBool(line[41])
		if err == nil {
			row.InspnIssued = inspnIssued
		}
		inspnReturned, err := strToBool(line[42])
		if err == nil {
			row.InspnReturned = inspnReturned
		}
		habitabilityRepairsRequired, err := strToBool(line[43])
		if err == nil {
			row.HabitabilityRepairsRequired = habitabilityRepairsRequired
		}
		destroyed, err := strToBool(line[47])
		if err == nil {
			row.Destroyed = destroyed
		}
		floodDamage, err := strToBool(line[50])
		if err == nil {
			row.FloodDamage = floodDamage
		}
		foundationDamage, err := strToBool(line[52])
		if err == nil {
			row.FoundationDamage = foundationDamage
		}
		roofDamage, err := strToBool(line[54])
		if err == nil {
			row.RoofDamage = roofDamage
		}
		tsaEligible, err := strToBool(line[56])
		if err == nil {
			row.TsaEligible = tsaEligible
		}
		tsaCheckedIn, err := strToBool(line[57])
		if err == nil {
			row.TsaCheckedIn = tsaCheckedIn
		}
		rentalAssistanceEligible, err := strToBool(line[58])
		if err == nil {
			row.RentalAssistanceEligible = rentalAssistanceEligible
		}
		repairAssistanceEligible, err := strToBool(line[60])
		if err == nil {
			row.RepairAssistanceEligible = repairAssistanceEligible
		}
		replacementAssistanceEligible, err := strToBool(line[62])
		if err == nil {
			row.ReplacementAssistanceEligible = replacementAssistanceEligible
		}
		personalPropertyEligible, err := strToBool(line[64])
		if err == nil {
			row.PersonalPropertyEligible = personalPropertyEligible
		}
		ihpMax, err := strToBool(line[66])
		if err == nil {
			row.IhpMax = ihpMax
		}
		haMax, err := strToBool(line[67])
		if err == nil {
			row.HaMax = haMax
		}
		onaMax, err := strToBool(line[68])
		if err == nil {
			row.OnaMax = onaMax
		}

		db.Clauses(clause.OnConflict{
			UpdateAll: true,
		}).Create(&row)
		rowCount++
		fmt.Printf("\r %f %% done ... just upserted %s", (float64(rowCount) / float64(totalToProcess) * 100), row.ID)
	}
}

// convert string to date
func strToDate(str string) time.Time {
	t, err := time.Parse("2006-01-02T15:04:05.999999999Z07:00", str)
	if err != nil {
		log.Fatal(fmt.Sprintf("Error converting %s to date", str))
		panic(err)
	}
	return t
}

// convert a string to a bool
func strToBool(str string) (bool, error) {
	b, err := strconv.ParseBool(str)
	if err != nil {
		return false, err
	}
	return b, nil
}

//convert string to float
func strToFloat(str string) float64 {
	f, err := strconv.ParseFloat(str, 64)
	if err != nil {
		log.Fatal(fmt.Sprintf("Error converting %s to int", str))
		panic(err)
	}
	return f
}

func lineCounter(r io.Reader) (int, error) {
	buf := make([]byte, 32*1024)
	count := 0
	lineSep := []byte{'\n'}

	for {
		c, err := r.Read(buf)
		count += bytes.Count(buf[:c], lineSep)

		switch {
		case err == io.EOF:
			return count, nil

		case err != nil:
			return count, err
		}
	}
}
