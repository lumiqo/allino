package allino

import "encoding/json"

// jobUser is a snapshot of the authenticated caller, not a bearer token.
// It deliberately does not affect job identity or appear in JobStore responses.
type jobUser struct {
	UID          string `json:"uid"`
	Name         string `json:"name"`
	Writable     bool   `json:"writable"`
	AuthorizedBy string `json:"authorized_by"`
}

func (r *Runtime) snapshotJobUser() *jobUser {
	uid, name, writable, err := r.User()
	if err != nil {
		return nil
	}
	return &jobUser{UID: uid, Name: name, Writable: writable, AuthorizedBy: r.AuthorizedBy()}
}

func (r *Runtime) restoreJobUser(user *jobUser) {
	if user == nil {
		return
	}
	r.AssumeUser(user.UID, user.Name, user.Writable)
	r.cache.authorizedBy = user.AuthorizedBy
}

func encodeJobUser(user *jobUser) any {
	if user == nil {
		return nil
	}
	data, _ := json.Marshal(user) // Only strings and a bool; marshaling cannot fail.
	return string(data)
}
