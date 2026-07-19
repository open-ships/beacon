// Package identity owns Beacon's stable ISO 11783/NMEA 2000 appliance
// identity and the product/configuration information derived from it.
package identity

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	n2k "github.com/open-ships/n2k"

	"github.com/open-ships/beacon/internal/store"
)

const (
	N2KVersion               = 3000
	ExperimentalManufacturer = 2000
)

type Appliance struct {
	IdentityNumber   uint32    `json:"identity_number"`
	ManufacturerCode uint16    `json:"manufacturer_code"`
	ProductCode      uint16    `json:"product_code"`
	DeviceClass      uint8     `json:"device_class"`
	DeviceFunction   uint8     `json:"device_function"`
	IndustryGroup    uint8     `json:"industry_group"`
	N2KVersion       uint16    `json:"n2k_version"`
	Certification    uint8     `json:"certification_level"`
	SerialNumber     string    `json:"serial_number"`
	DeviceNAME       uint64    `json:"device_name"`
	DeviceNameHex    string    `json:"device_name_hex"`
	CreatedAt        time.Time `json:"created_at"`
	Conformance      string    `json:"conformance"`
}

func LoadOrCreate(ctx context.Context, st *store.Store) (Appliance, error) {
	var number int64
	var created int64
	err := st.DB().QueryRowContext(ctx,
		`SELECT identity_number, created_at FROM appliance_identity WHERE id = 1`).Scan(&number, &created)
	if errors.Is(err, sql.ErrNoRows) {
		number = int64(random21Bit())
		created = time.Now().UTC().UnixNano()
		if _, err = st.DB().ExecContext(ctx,
			`INSERT OR IGNORE INTO appliance_identity(id, identity_number, created_at) VALUES (1, ?, ?)`,
			number, created); err != nil {
			return Appliance{}, fmt.Errorf("persist appliance identity: %w", err)
		}
		err = st.DB().QueryRowContext(ctx,
			`SELECT identity_number, created_at FROM appliance_identity WHERE id = 1`).Scan(&number, &created)
	}
	if err != nil {
		return Appliance{}, fmt.Errorf("load appliance identity: %w", err)
	}
	return build(uint32(number), time.Unix(0, created).UTC()), nil
}

func random21Bit() uint32 {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return nonzero21Bit(uint32(time.Now().UnixNano()))
	}
	return nonzero21Bit(binary.LittleEndian.Uint32(b[:]))
}

func nonzero21Bit(v uint32) uint32 {
	v &= 0x1FFFFF
	if v == 0 {
		return 1
	}
	return v
}

func build(number uint32, created time.Time) Appliance {
	name := n2k.DeviceName{
		IdentityNumber: number, ManufacturerCode: ExperimentalManufacturer,
		DeviceClass: 25, DeviceFunction: 130, IndustryGroup: 4,
	}
	packed := name.Pack(true)
	return Appliance{
		IdentityNumber: number, ManufacturerCode: ExperimentalManufacturer,
		DeviceClass: 25, DeviceFunction: 130, IndustryGroup: 4,
		N2KVersion: N2KVersion, SerialNumber: fmt.Sprintf("beacon-%06x", number),
		DeviceNAME: packed, DeviceNameHex: fmt.Sprintf("%016X", packed), CreatedAt: created,
		Conformance: "experimental-unassigned",
	}
}

func (a Appliance) Name() n2k.DeviceName {
	return n2k.DeviceName{
		IdentityNumber: a.IdentityNumber, ManufacturerCode: a.ManufacturerCode,
		DeviceClass: a.DeviceClass, DeviceFunction: a.DeviceFunction,
		IndustryGroup: a.IndustryGroup,
	}
}

func (a Appliance) Options(version string) []n2k.Option {
	return []n2k.Option{
		n2k.WithName(a.Name()),
		n2k.WithProductInfo(n2k.ProductInfo{
			N2KVersion: a.N2KVersion, ProductCode: a.ProductCode,
			ModelID: "Open Ships Beacon", SoftwareVersion: version,
			ModelVersion: "N2K integration gateway", SerialNumber: a.SerialNumber,
			CertificationLevel: a.Certification, LoadEquivalency: 1,
		}),
		n2k.WithConfigInfo(n2k.ConfigInfo{
			InstallationDescription1: "Open Ships Beacon gateway",
			InstallationDescription2: a.Conformance,
			ManufacturerInformation:  "openships.ai",
		}),
	}
}
