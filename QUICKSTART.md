# Quick Start Guide - ADF Pipeline Clone Extension

## 🎉 Your extension is ready!

The ADF Pipeline Clone extension has been successfully created with a beautiful, fully functional visual pipeline editor.

## 🚀 How to Run the Extension

1. **Press F5** - This will:
   - Compile the extension
   - Open a new VS Code window with your extension loaded
   - The Extension Development Host window will appear

2. **Open the Pipeline Editor**:
   - Press `Ctrl+Shift+P` (or `Cmd+Shift+P` on Mac)
   - Type: **"ADF: Open Pipeline Editor"**
   - Press Enter

3. **Start Building Your Pipeline**:
   - Drag activities from the left sidebar onto the canvas
   - Hold `Shift` + Click on an activity, then click another to create connections
   - Click activities to edit their properties in the right panel
   - Right-click for quick actions

## ✨ What's Included

### Features
- ✅ Beautiful canvas-based pipeline editor
- ✅ 9 different activity types organized by category
- ✅ Drag-and-drop functionality
- ✅ Visual connections with smooth bezier curves
- ✅ Properties panel for editing
- ✅ Zoom, pan, and fit-to-screen controls
- ✅ Context menu with quick actions
- ✅ Grid background for alignment
- ✅ VS Code theme integration

### Zero External UI Dependencies
Built entirely with:
- Pure TypeScript
- Canvas API for rendering
- VS Code Extension API
- No React, no react-flow, no external UI libraries

## 🎨 Keyboard Shortcuts & Controls

| Action | How To |
|--------|--------|
| Add Activity | Drag from sidebar to canvas |
| Move Activity | Click and drag on canvas |
| Create Connection | `Shift` + Click source → Click target |
| Select Activity | Click on activity |
| Delete Activity | Right-click → Delete |
| Save Pipeline | Click 💾 Save button |
| Clear Canvas | Click 🗑️ Clear button |
| Zoom In/Out | Use toolbar buttons |

## 📂 Project Structure

```
adf-clone-extension/
├── src/
│   ├── extension.ts           # Main extension entry point
│   ├── pipelineEditor.ts      # Webview provider & HTML content
│   └── test/                  # Test files
├── .vscode/                   # VS Code configuration
├── package.json               # Extension manifest
├── tsconfig.json              # TypeScript configuration
└── README.md                  # Documentation
```

## 🛠️ Development Commands

```bash
# Compile TypeScript
npm run compile

# Watch mode (auto-compile on changes)
npm run watch

# Run tests
npm test

# Package extension
npm run package
```

## 🎯 Next Steps

1. **Test the extension** - Press F5 and try the pipeline editor
2. **Customize activities** - Add more activity types in `pipelineEditor.ts`
3. **Add persistence** - Implement save/load functionality
4. **Enhance features** - Add validation, templates, or export options

## 📝 Key Files to Modify

- **`src/extension.ts`** - Extension activation & commands
- **`src/pipelineEditor.ts`** - Canvas UI, rendering, and interactions
- **`package.json`** - Extension metadata and commands

## 💡 Tips

- The canvas uses `getContext('2d')` for all rendering
- Activities are stored as JavaScript objects with x, y coordinates
- Connections are drawn using bezier curves for smooth lines
- Properties panel updates in real-time as you type
- All styling uses VS Code CSS variables for theme compatibility

## 🐛 Debugging

- Set breakpoints in TypeScript files
- Use `console.log()` in extension code
- Check Debug Console for output
- Use `vscode.postMessage()` to communicate between webview and extension

## 🎨 Customization Ideas

- Add more activity types (Azure Functions, Logic Apps, etc.)
- Implement copy/paste functionality
- Add keyboard shortcuts for common actions
- Create activity templates
- Add pipeline validation
- Export to JSON or ARM template format
- Add undo/redo functionality

---

**Enjoy building beautiful data pipelines!** 🚀

For more information, see [VS Code Extension API](https://code.visualstudio.com/api)
