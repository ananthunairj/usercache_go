package src

import (
    "sync"
    "testing"
    "time"
)

func TestUserManager_AddNewUser_Success(t *testing.T) {
    um := &UserManager{
        Users: make(map[string]*User),
        Mu:    &sync.RWMutex{},
    }
    
    sessionExpiry := 30 * time.Minute
    refreshExpiry := 24 * time.Hour
    memoryLimit := 100.0 // 100 MB
    
    user, err := um.AddNewUser(sessionExpiry, refreshExpiry, memoryLimit)
    
    if err != nil {
        t.Fatalf("Expected no error, got: %v", err)
    }
    
    if user == nil {
        t.Fatal("Expected user to be created, got nil")
    }
    
    if user.Id == "" {
        t.Error("Expected user ID to be set")
    }
    
    if user.CurrentSessionId == "" {
        t.Error("Expected current session ID to be set")
    }
    
    if len(user.Sessions) != 1 {
        t.Errorf("Expected 1 session, got %d", len(user.Sessions))
    }
    
    if !user.isActive {
        t.Error("Expected user to be active")
    }
    
    if user.SharedCache == nil {
        t.Error("Expected shared cache to be initialized")
    }
    
    if user.memory == nil {
        t.Error("Expected memory limit to be set")
    }
    
    // Verify user is in UserManager
    um.Mu.RLock()
    _, exists := um.Users[user.Id]
    um.Mu.RUnlock()
    
    if !exists {
        t.Error("Expected user to be added to UserManager")
    }
}

func TestUserManager_AddNewUser_InvalidMemoryLimit_Zero(t *testing.T) {
    um := &UserManager{
        Users: make(map[string]*User),
        Mu:    &sync.RWMutex{},
    }
    
    sessionExpiry := 30 * time.Minute
    refreshExpiry := 24 * time.Hour
    memoryLimit := 0.0
    
    user, err := um.AddNewUser(sessionExpiry, refreshExpiry, memoryLimit)
    
    if err != errMemoryLimit {
        t.Errorf("Expected errMemoryLimit, got: %v", err)
    }
    
    if user != nil {
        t.Error("Expected user to be nil when memory limit is invalid")
    }
}

func TestUserManager_AddNewUser_InvalidMemoryLimit_Negative(t *testing.T) {
    um := &UserManager{
        Users: make(map[string]*User),
        Mu:    &sync.RWMutex{},
    }
    
    sessionExpiry := 30 * time.Minute
    refreshExpiry := 24 * time.Hour
    memoryLimit := -50.0
    
    user, err := um.AddNewUser(sessionExpiry, refreshExpiry, memoryLimit)
    
    if err != errMemoryLimit {
        t.Errorf("Expected errMemoryLimit, got: %v", err)
    }
    
    if user != nil {
        t.Error("Expected user to be nil when memory limit is negative")
    }
}

func TestUserManager_AddNewUser_MemoryExceeded(t *testing.T) {
    um := &UserManager{
        Users: make(map[string]*User),
        Mu:    &sync.RWMutex{},
    }
    
    sessionExpiry := 30 * time.Minute
    refreshExpiry := 24 * time.Hour
    memoryLimit := 999999.0 // Extremely high memory limit
    
    user, err := um.AddNewUser(sessionExpiry, refreshExpiry, memoryLimit)
    
    if err != errMemExceeded {
        t.Errorf("Expected errMemExceeded, got: %v", err)
    }
    
    if user != nil {
        t.Error("Expected user to be nil when memory limit exceeds available memory")
    }
}

func TestUserManager_AddNewUser_SessionConfiguration(t *testing.T) {
    um := &UserManager{
        Users: make(map[string]*User),
        Mu:    &sync.RWMutex{},
    }
    
    sessionExpiry := 30 * time.Minute
    refreshExpiry := 24 * time.Hour
    memoryLimit := 100.0
    
    user, err := um.AddNewUser(sessionExpiry, refreshExpiry, memoryLimit)
    
    if err != nil {
        t.Fatalf("Expected no error, got: %v", err)
    }
    
    session := user.Sessions[user.CurrentSessionId]
    
    if session == nil {
        t.Fatal("Expected session to be created")
    }
    
    if !session.isActive {
        t.Error("Expected session to be active")
    }
    
    if session.cache == nil {
        t.Error("Expected session cache to be initialized")
    }
    
    if session.lastAccessed.IsZero() {
        t.Error("Expected lastAccessed to be set")
    }
    
    if session.SessionId != user.CurrentSessionId {
        t.Error("Expected session ID to match current session ID")
    }
}

func TestUserManager_AddNewUser_ConcurrentAccess(t *testing.T) {
    um := &UserManager{
        Users: make(map[string]*User),
        Mu:    &sync.RWMutex{},
    }
    
    sessionExpiry := 30 * time.Minute
    refreshExpiry := 24 * time.Hour
    memoryLimit := 50.0
    
    numGoroutines := 10
    var wg sync.WaitGroup
    errors := make(chan error, numGoroutines)
    users := make(chan *User, numGoroutines)
    
    for i := 0; i < numGoroutines; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            user, err := um.AddNewUser(sessionExpiry, refreshExpiry, memoryLimit)
            if err != nil {
                errors <- err
                return
            }
            users <- user
        }()
    }
    
    wg.Wait()
    close(errors)
    close(users)
    
    // Check for errors
    for err := range errors {
        if err != nil && err != errMemExceeded {
            t.Errorf("Unexpected error during concurrent access: %v", err)
        }
    }
    
    // Verify unique user IDs
    userIds := make(map[string]bool)
    for user := range users {
        if userIds[user.Id] {
            t.Errorf("Duplicate user ID found: %s", user.Id)
        }
        userIds[user.Id] = true
    }
}

func TestUserManager_AddNewUser_TokenGeneration(t *testing.T) {
    um := &UserManager{
        Users: make(map[string]*User),
        Mu:    &sync.RWMutex{},
    }
    
    sessionExpiry := 30 * time.Minute
    refreshExpiry := 24 * time.Hour
    memoryLimit := 100.0
    
    // Create multiple users
    users := make([]*User, 3)
    for i := 0; i < 3; i++ {
        user, err := um.AddNewUser(sessionExpiry, refreshExpiry, memoryLimit)
        if err != nil {
            t.Fatalf("Failed to create user %d: %v", i, err)
        }
        users[i] = user
    }
    
    // Verify all tokens are unique
    tokens := make(map[string]bool)
    for _, user := range users {
        if tokens[user.Id] {
            t.Errorf("Duplicate user ID found: %s", user.Id)
        }
        tokens[user.Id] = true
        
        if tokens[user.CurrentSessionId] {
            t.Errorf("Duplicate session ID found: %s", user.CurrentSessionId)
        }
        tokens[user.CurrentSessionId] = true
    }
}

func TestUserManager_AddNewUser_MemoryLimitConfiguration(t *testing.T) {
    um := &UserManager{
        Users: make(map[string]*User),
        Mu:    &sync.RWMutex{},
    }
    
    testCases := []struct {
        name        string
        memoryLimit float64
        expectError bool
    }{
        {"Small memory limit", 10.0, false},
        {"Medium memory limit", 100.0, false},
        {"Large memory limit", 500.0, false},
        {"Zero memory limit", 0.0, true},
        {"Negative memory limit", -10.0, true},
    }
    
    for _, tc := range testCases {
        t.Run(tc.name, func(t *testing.T) {
            user, err := um.AddNewUser(30*time.Minute, 24*time.Hour, tc.memoryLimit)
            
            if tc.expectError {
                if err == nil {
                    t.Error("Expected error, got nil")
                }
                if user != nil {
                    t.Error("Expected user to be nil on error")
                }
            } else {
                if err != nil && err != errMemExceeded {
                    t.Errorf("Unexpected error: %v", err)
                }
                if user != nil && user.memory.configured != mbSizeToUINT(tc.memoryLimit) {
                    t.Errorf("Expected memory limit %d, got %d", 
                        mbSizeToUINT(tc.memoryLimit), 
                        user.memory.configured)
                }
            }
        })
    }
}

func TestUserManager_AddNewUser_UserSnapshot(t *testing.T) {
    um := &UserManager{
        Users: make(map[string]*User),
        Mu:    &sync.RWMutex{},
    }
    
    sessionExpiry := 30 * time.Minute
    refreshExpiry := 24 * time.Hour
    memoryLimit := 100.0
    
    user, err := um.AddNewUser(sessionExpiry, refreshExpiry, memoryLimit)
    
    if err != nil {
        t.Fatalf("Expected no error, got: %v", err)
    }
    
    // Verify snapshot data matches user data
    if user.Id == "" {
        t.Error("Expected user ID to be set in snapshot")
    }
    
    if user.CurrentSessionId == "" {
        t.Error("Expected current session ID to be set in snapshot")
    }
    
    if !user.isActive {
        t.Error("Expected user to be active in snapshot")
    }
}

// Benchmark tests
func BenchmarkUserManager_AddNewUser(b *testing.B) {
    um := &UserManager{
        Users: make(map[string]*User),
        Mu:    &sync.RWMutex{},
    }
    
    sessionExpiry := 30 * time.Minute
    refreshExpiry := 24 * time.Hour
    memoryLimit := 100.0
    
    b.ResetTimer()
    
    for i := 0; i < b.N; i++ {
        _, _ = um.AddNewUser(sessionExpiry, refreshExpiry, memoryLimit)
    }
}